package raw.executor

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.Timer
import org.apache.spark.rdd.RDD
import raw.datasets.AccessPath
import raw.utils.{Instrumented, RawUtils}
import raw.{QueryLogger, RawQuery}

import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Global, Settings}

object RawCompiler {
  val defaultTargetDirectory = RawUtils.getTemporaryDirectory("rawqueries")
  private[this] val ai = new AtomicInteger()

  private def newClassName(): String = {
    "Query" + ai.getAndIncrement()
  }
}

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

    settings.usejavacp.value = true
    //settings.showPlugins only works if you're not compiling a file, same as -help
    logger.info(s"Compiling queries to directory: $classOutputDir")
    settings.d.tryToSet(List(classOutputDir.toString))
    settings
  }

  private[this] val compileReporter = new StoreReporter()

  private[this] val compiler = new Global(compilerSettings, compileReporter)

  /**
   * @return An instance of the query
   * @throws RuntimeException If compilation fails
   */
  def compileOQL(oql: String, accessPaths: Seq[AccessPath[_]]): RawQuery = {
    compile("oql", oql, accessPaths)
  }

  def compileLogicalPlan(plan: String, accessPaths: Seq[AccessPath[_]]): RawQuery = {
    compile("plan", plan, accessPaths)
  }

  def compileLogicalPlan2(plan: String, scanners: Set[RawScanner[_]]): RawQuery = {
    compile2("plan", plan, scanners)
  }


  private[this] def getContainerClass(accessPath: AccessPath[_]): Class[_] = {
    accessPath.path match {
      case Left(list) => classOf[List[_]]
      case Right(rdd) => classOf[RDD[_]]
    }
  }

  // TODO: RDD support?
  private[this] def getContainerClass2(scanner: RawScanner[_]): Class[_] = classOf[Iterable[_]]

  private[this] def getAccessPaths(accessPath: AccessPath[_]): Object = {
    accessPath.path match {
      case Left(list) => list
      case Right(rdd) => rdd
    }
  }

  // TODO: Remove usages of AccessPath and use only RawScanner (Iterable). Must update how tests load files.
  private[this] def compile(queryFieldName: String, query: String, accessPaths: Seq[AccessPath[_]]): RawQuery = {
    //    logger.info("Access paths: " + accessPaths)
    val queryName = RawCompiler.newClassName()
    val aps: Seq[String] = accessPaths.map(ap => ap.tag.tpe.typeSymbol.fullName)
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
    val args = accessPaths.map(ap => s"${ap.name}: ${getContainerClass(ap).getSimpleName}[${ap.tag.tpe.typeSymbol.name}]").mkString(", ")
    //    val args = accessPaths.map(ap => s"${ap.name}: ${getContainerClass(ap).getSimpleName}[${ap.tag.tpe.typeSymbol.fullName}]").mkString(", ")

    val code = s"""
package raw.query

import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
$imports

@rawQueryAnnotation
class $queryName($args) extends RawQuery {
  val $queryFieldName =
  \"\"\"
  $query
  \"\"\"
}
"""

    logger.info(s"Generated code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(queryName + ".scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Compiling source file: ${srcFile.toAbsolutePath}")


    queryCompileTimer.time {
      // Compile the query
      val run = new compiler.Run()
      run.compile(List(srcFile.toString))
    }

    if (compileReporter.hasErrors) {
      // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
      // compilation runs are not falsely reported in the subsequent runs
      val message = "Query compilation failed. Compilation messages:\n" + compileReporter.infos.mkString("\n")
      compileReporter.reset()
      return throw new RuntimeException(message)
    }

    // Load the main query class
    val queryClass = s"raw.query.$queryName"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)

    // Find the constructor
    val ctorTypeArgs: Seq[Class[_]] = accessPaths.map(ap => getContainerClass(ap))
    val ctor = clazz.getConstructor(ctorTypeArgs: _*)

    // Create an instance of the query using the container instances (RDDs or List) given in the access paths.
    val ctorArgs: Seq[Object] = accessPaths.map(ap => getAccessPaths(ap))
    ctor.newInstance(ctorArgs: _*).asInstanceOf[RawQuery]
  }

  private[this] def compile2(queryFieldName: String, query: String, scanners: Set[RawScanner[_]]): RawQuery = {
    //    logger.info("Access paths: " + accessPaths)
    val queryName = RawCompiler.newClassName()
    val aps: Set[String] = scanners.map(scanner => scanner.tag.tpe.typeSymbol.fullName)
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
      .map(scanner => s"${scanner.schema.name}: ${getContainerClass2(scanner).getSimpleName}[${scanner.tag.tpe.typeSymbol.name}]")
      .mkString(", ")
    //    val args = accessPaths.map(ap => s"${ap.name}: ${getContainerClass(ap).getSimpleName}[${ap.tag.tpe.typeSymbol.fullName}]").mkString(", ")

    val code = s"""
package raw.query

import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
$imports

@rawQueryAnnotation
class $queryName($args) extends RawQuery {
  val $queryFieldName =
  \"\"\"
  $query
  \"\"\"
}
"""

    logger.info(s"Generated code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(queryName + ".scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Compiling source file: ${srcFile.toAbsolutePath}")


    queryCompileTimer.time {
      // Compile the query
      val run = new compiler.Run()
      run.compile(List(srcFile.toString))
    }

    if (compileReporter.hasErrors) {
      // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
      // compilation runs are not falsely reported in the subsequent runs
      val message = "Query compilation failed. Compilation messages:\n" + compileReporter.infos.mkString("\n")
      compileReporter.reset()
      return throw new RuntimeException(message)
    }

    // Load the main query class
    val queryClass = s"raw.query.$queryName"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)

    // Find the constructor. Transform scanners from a set to a sequence because after type erasure, all scanners are
    // instances of Iterable[_] or RDD[_]
    val ctorTypeArgs: Seq[Class[_]] = scanners.toSeq.map(ap => getContainerClass2(ap))
    logger.info("ctorTypeArgs: " + ctorTypeArgs + " " + ctorTypeArgs.toSeq)
    val ctor = clazz.getConstructor(ctorTypeArgs.toSeq: _*)

    // Create an instance of the query using the container instances (Iterators) given in the access paths.
    val ctorArgs: Seq[Iterable[_]] = scanners.toSeq
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

    val queryClass = s"raw.query.${className}"
    logger.info("Creating new instance of: " + queryClass)
    val clazz = rawClassloader.loadClass(queryClass)

    val ctor = clazz.getConstructor()

    val ctorArgs: List[Object] = List()
    ctor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]
  }
}
