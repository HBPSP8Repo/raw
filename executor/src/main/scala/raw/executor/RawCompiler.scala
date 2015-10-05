package raw.executor

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

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
class CompilationException(val queryError: QueryError) extends Exception


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

  private[this] val compileReporter = new StoreReporter()

  //  private[this] val compiler = new Global(compilerSettings, new ConsoleReporter(compilerSettings))
  private[this] val compiler = new Global(compilerSettings, compileReporter)
  private[this] val compilerLock = new Object

  /**
   * @return An instance of the query
   * @throws RuntimeException If compilation fails
   */
  def compileOQL(oql: String, scanners: Seq[RawScanner[_]]): RawQuery = {
    compile(OQL, oql, scanners)
  }

  def compileLogicalPlan(plan: String, scanners: Seq[RawScanner[_]]): RawQuery = {
    compile(LogicalPlan, plan, scanners)
  }


  private[this] def getContainerClass(scanner: RawScanner[_]): Class[_] = {
    scanner match {
      case x: SparkRawScanner[_] => classOf[RDD[_]]
      case _ => classOf[RawScanner[_]]
    }
  }

  private[this] def getAccessPaths(scanner: RawScanner[_]): Object = {
    scanner match {
      case x: SparkRawScanner[_] => x.toRDD()
      case _ => scanner
    }
  }


  def compile(queryLanguage: QueryLanguage, query: String, scanners: Seq[RawScanner[_]]): RawQuery = {
    logger.info("Scanners: " + scanners)
    val queryName = RawCompiler.newClassName()
    val aps: Seq[String] = scanners.map(scanner => scanner.tt.tpe.typeSymbol.fullName)
    logger.info(s"Access paths: $aps")

    /* For every top level type argument of the access path, import the containing package. The is, for the following
     * access paths: RDD[raw.Publications], RDD[raw.patients.Patient], generate "import raw._" and "import raw.patients._"
     *
     * NOTE: this does not check nested types, that is, if Patient contains references to a Diagnostic instance,
     * it will only import the package of Patient. Therefore, any nested types should be in the same package as the
     * top level type. This limitation can be eliminated by using TypeTags instead of ClassTags in the AccessPath class
     * and by recursively scanning the full type of the access path.
     */
    val imports = aps.map(ap => ap.lastIndexOf(".") match {
      case -1 => throw new RuntimeException(s"Case classes in access paths should not be at top level package: $ap")
      case i: Int => "import " + ap.substring(0, i + 1) + "_"
    }).toSet.mkString("\n")

    //    val imports = accessPaths.map(ap => s"import ${ap.tag.toString()}").mkString("\n")
    //    val args = accessPaths.map(ap => s"${ap.name}: RDD[${ap.tag.runtimeClass.getSimpleName}]").mkString(", ")
    val args = scanners
      .map(scanner => {
      val containerName = getContainerClass(scanner).getSimpleName
      val containerTypeParameter = scanner.tt.tpe.finalResultType
      s"${scanner.schema.name}: ${containerName}[${containerTypeParameter}]"
    })
      .mkString(", ")
    //    val args = accessPaths.map(ap => s"${ap.name}: ${getContainerClass(ap).getSimpleName}[${ap.tag.tpe.typeSymbol.fullName}]").mkString(", ")

    val code = s"""
package raw.query

import raw.executor.RawScanner
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
$imports

@rawQueryAnnotation
class $queryName($args) extends RawQuery {
  val ${queryLanguage.name} =
  \"\"\"$query\"\"\"
}
"""

    logger.info(s"Generated code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(queryName + ".scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Compiling source file: ${srcFile.toAbsolutePath}")

    compilerLock.synchronized {
      queryCompileTimer.time {
        // Compile the query
        val run = new compiler.Run()
        run.compile(List(srcFile.toString))
      }

      if (compileReporter.hasWarnings || compileReporter.hasErrors) {
        logger.warn("Errors during compilation:\n " + compileReporter.infos.mkString("\n"))
        // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
        // compilation runs are not falsely reported in the subsequent runs
        compileReporter.reset()
        RawImpl.queryError.get() match {
          case Some(queryError: QueryError) => throw new CompilationException(queryError)
          case None => logger.warn("Compiler has warnings or errors but no query error object available.")
        }
      }
    }

    // Load the main query class
    val queryClass = s"raw.query.$queryName"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)

    // Find the constructor.
    val ctorTypeArgs: Seq[Class[_]] = scanners.map(ap => getContainerClass(ap))
    logger.info("ctorTypeArgs: " + ctorTypeArgs + " " + ctorTypeArgs.toSeq)
    val ctor = clazz.getConstructor(ctorTypeArgs.toSeq: _*)


    // Create an instance of the query using the container instances (RDDs or List) given in the access paths.
    val ctorArgs: Seq[Object] = scanners.map(scanner => getAccessPaths(scanner))
    ctor.newInstance(ctorArgs: _*).asInstanceOf[RawQuery]
  }

  /**
   *
   * @param code
   * @param className Name of the class defined in the source code. Must be unique within the current run of the program
   * @return
   */
  def compileLoader(code: String, className: String): AnyRef = {
    logger.info(s"Loader source code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(s"$className.scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))

    logger.info(s"Compiling source file: ${srcFile.toAbsolutePath}")
    compilerLock.synchronized {
      loaderCompileTimer.time {
        val run = new compiler.Run()
        run.compile(List(srcFile.toString))
      }
      if (compileReporter.hasErrors) {
        // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
        // compilation runs are not falsely reported in the subsequent runs
        val message = "Compilation of data loader failed. Compilation messages:\n" + compileReporter.infos.mkString("\n")
        compileReporter.reset()
        return throw new RuntimeException(message)
      }
    }
    val queryClass = s"raw.query.${className}"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)

    val ctor = clazz.getConstructor()

    val ctorArgs: List[Object] = List()
    ctor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]
  }
}
