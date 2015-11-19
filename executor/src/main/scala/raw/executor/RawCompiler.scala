package raw.executor

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.Timer
import org.apache.spark.rdd.RDD
import raw.QueryLanguages.{LogicalPlan, OQL, QueryLanguage}
import raw._
import raw.utils.{Instrumented, RawUtils}

import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Global, Settings}

object RawCompiler {
  final val defaultTargetDirectory = RawUtils.getTemporaryDirectory("rawqueries")
  private[this] val ai = new AtomicInteger()

  private def newClassName(): String = "Query" + ai.getAndIncrement()

}

// Represents an error during query compilation.
class CompilationException(msg: String, val queryError: QueryError) extends Exception(msg) {
  def this(queryError: QueryError) = this("", queryError)
}

class InternalException(cause: Throwable) extends Exception(cause)

class RawCompiler(val rawClassloader: RawMutableURLClassLoader,
                  val baseOutputDir: Path = RawCompiler.defaultTargetDirectory) extends StrictLogging with Instrumented {
  // Metrics
  private[this] val queryCompileTimer: Timer = metrics.timer("compile:query")
  private[this] val loaderCompileTimer: Timer = metrics.timer("compile:loader")

  RawUtils.cleanOrCreateDirectory(baseOutputDir)

  // Where the server saves the generated scala source for each query
  private[this] val sourceOutputDir: Path = {
    val dir = baseOutputDir.resolve("src")
    Files.createDirectory(dir)
    dir
  }

  // Where the compiler writes the generated classes that implement the queries.
  private[this] val classOutputDir: Path = {
    val dir = baseOutputDir.resolve("classes")
    Files.createDirectory(dir)
    // Point the classloader to the directory with the query classes.
    this.rawClassloader.addURL(dir.toUri.toURL)
    dir
  }

  private[this] val macroGenerated = {
    val dir = baseOutputDir.resolve("macro-generated")
    //    if (!Files.exists(dir)) {
    Files.createDirectory(dir)
    //    }
    QueryLogger.setOutputDirectory(dir)
    dir
  }

  private[this] val compilerSettings = {
    val settings = new Settings
    settings.embeddedDefaults[RawCompiler]

    // Needed for macro annotations
    val p = RawUtils.extractResource("paradise_2.11.7-2.1.0-M5.jar")
    logger.info("Loading plugin: " + p)
    settings.plugin.tryToSet(List(p.toString))
    settings.require.tryToSet(List("macroparadise"))

    //    settings.debug.tryToSet(List("true"))
    //    settings.showPhases.tryToSet(List("true"))
    //    settings.verbose.tryToSet(List("true"))
    //    settings.explaintypes.tryToSet(List("true"))
    //    settings.printtypes.tryToSet(List("true"))
    //    settings.Xshowobj.tryToSet(List("true"))
    //    settings.Xshowcls.tryToSet(List("true"))
    //    settings.Xshowtrees.tryToSet(List("true"))
    //    settings.YmacrodebugVerbose.tryToSet(List("true"))
    //    settings.Ypatmatdebug.tryToSet(List("true"))
    //    settings.Yreifydebug.tryToSet(List("true"))
    //    settings.Yposdebug.tryToSet(List("true"))

    settings.usejavacp.value = true
    //settings.showPlugins only works if you're not compiling a file, same as -help
    logger.info(s"Compiling queries to directory: $classOutputDir")
    settings.d.tryToSet(List(classOutputDir.toString))
    settings
  }

  // TODO: StoreReporter does not print all compilation messages, the ConsoleReporter is better but
  // we cannot easily intersect the output like with the store reporter. Check if we can combine them.
  private[this] val compileReporter = new StoreReporter()

//    private[this] val compiler = new Global(compilerSettings, new ConsoleReporter(compilerSettings))
  private[this] val compiler = new Global(compilerSettings, compileReporter)
  private[this] val compilerLock = new Object

  /**
    * @return An instance of the query
    * @throws RuntimeException If compilation fails
    */
  def compileOQL(oql: String, user:String): RawQuery = {
    compile(OQL, oql, user)
  }

  def compileLogicalPlan(plan: String, user:String): RawQuery = {
    compile(LogicalPlan, plan, user)
  }


  private[this] def getContainerClass(scanner: RawScanner[_]): Class[_] = {
    scanner match {
      case x: SparkRawScanner[_] => classOf[RDD[_]]
      case _ => classOf[RawScanner[_]]
    }
  }

  def compile(queryLanguage: QueryLanguage, query: String, user:String): RawQuery = {
    def descapeStr(s: String) = {
      var descapedStr = ""
      for (c <- s) {
        descapedStr += (c match {
          case '\\' => "\\\\"
          case '\'' => "\\'"
          case '\"' => "\\\""
          case '\b' => "\\b"
          case '\f' => "\\f"
          case '\n' => "\\n"
          case '\r' => "\\r"
          case '\t' => "\\t"
          case _ => c
        })
      }
      descapedStr
    }

    logger.info("Building query")
    val queryName = RawCompiler.newClassName()

    val code =
      s"""package raw.query

import raw.executor.RawScanner
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}

@rawQueryAnnotation
class $queryName extends RawQuery {
  override val user = \"${user}\"
  val ${queryLanguage.name} =
  \"${descapeStr(query)}\"
}"""

    logger.info(s"Generated code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(queryName + ".scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Compiling source file: ${srcFile.toAbsolutePath}")

    val start = Stopwatch.createStarted()
    compilerLock.synchronized {
        // Compile the query
        val run = new compiler.Run()
        run.compile(List(srcFile.toString))

      if (compileReporter.hasWarnings || compileReporter.hasErrors) {
        logger.warn("Errors during compilation:\n " + compileReporter.infos.mkString("\n"))
        // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
        // compilation runs are not falsely reported in the subsequent runs
        compileReporter.reset()
        RawImpl.queryError.get() match {
          case Some(queryError: QueryError) => throw new CompilationException(queryError.toString, queryError)
          case Some(t: Throwable) => throw new InternalException(t)
          case None => logger.warn("Compiler has warnings or errors but no query error object available.")
        }
      }
    }
    val compilationTime = start.elapsed(TimeUnit.MILLISECONDS)
    queryCompileTimer.update(compilationTime, TimeUnit.MILLISECONDS)
    logger.info(s"Compilation time: $compilationTime ms")

    // Load the main query class
    val queryClass = s"raw.query.$queryName"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)
    val ctor = clazz.getConstructor()
    val ctorArgs: List[Object] = List.empty
    val rawQuery = ctor.newInstance(ctorArgs: _*).asInstanceOf[RawQuery]

    // Create an instance of the query using the container instances (RDDs or List) given in the access paths.
//    val ctorArgs: Seq[Object] = scanners.map(scanner => getAccessPaths(scanner))
//    val rawQuery = ctor.newInstance(ctorArgs: _*).asInstanceOf[RawQuery]
    rawQuery.compilationTime = compilationTime
    rawQuery
  }
}
