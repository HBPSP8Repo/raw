package raw.executionserver

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.RawQuery

import scala.reflect.ClassTag
import scala.tools.nsc.reporters.StoreReporter
import scala.tools.nsc.{Global, Settings}

/*
http://stackoverflow.com/questions/8867766/scala-dynamic-object-class-loading
https://code.google.com/p/session-scala/source/browse/functionaltests/src/test/scala/uk/ac/ic/doc/sessionscala/compiler/RunCompiler.scala?r=82ee19e0ad897de370ecac0e0aab3986e3e287ff
 */
class ExecutionServer(val rawClassloader: RawMutableURLClassLoader, val sc: SparkContext) extends StrictLogging with ResultConverter {
  private[this] val baseOutputDir: Path = Files.createTempDirectory("rawqueries")

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

  private[this] val compilerSettings = {
    val settings = new Settings
    settings.embeddedDefaults[ExecutionServer]

    // Needed for macro annotations
    val mpPlugin: URL = Resources.getResource("paradise_2.11.7-2.1.0-M5.jar")
    val p = Paths.get(mpPlugin.toURI)
    logger.info("Loading plugin: " + p)
    settings.processArgumentString(s"-Xplugin:${p.toString}") match {
      case (false, xs) => throw new RuntimeException("Failed to set configuration: " + xs)
      case (true, _) =>
    }
    settings.require.tryToSet(List("macroparadise"))

    settings.usejavacp.value = true
    //settings.showPlugins only works if you're not compiling a file, same as -help
    logger.info(s"Compiling queries to directory: $classOutputDir")
    settings.d.tryToSet(List(classOutputDir.toString))
    settings
  }
  private[this] val compileReporter = new StoreReporter()

  private[this] val compiler = new Global(compilerSettings, compileReporter)

  private[this] val ai = new AtomicInteger()

  private[this] def newClassName(): String = {
    "Query" + ai.getAndIncrement()
  }


  def execute(plan: String, accessPaths: List[AccessPath[_]]): Either[String, Any] = {
    //    logger.info("Access paths: " + accessPaths)
    val queryName = newClassName()
    val imports = accessPaths.map(ap => s"import ${ap.tag.toString()}").mkString("\n")
    val args = accessPaths.map(ap => s"${ap.name}: RDD[${ap.tag.runtimeClass.getSimpleName}]").mkString(", ")

    val code = s"""
package raw.query

import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
$imports

@rawQueryAnnotation
class $queryName($args) extends RawQuery {
  val plan =
  \"\"\"
  $plan
  \"\"\"
}
"""

    logger.info(s"Generated code:\n$code")
    val srcFile: Path = sourceOutputDir.resolve(queryName + ".scala")
    Files.write(srcFile, code.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Wrote source file: ${srcFile.toAbsolutePath}")

    // Compile the query
    val run = new compiler.Run()
    run.compile(List(srcFile.toString))
    if (compileReporter.hasErrors) {
      // the reporter keeps the state between runs, so it must be explicitly reset so that errors from previous
      // compilation runs are not falsely reported in the subsequent runs
      val message = "Query compilation failed. Compilation messages:\n" + compileReporter.infos.mkString("\n")
      compileReporter.reset()
      return Left(message)
    }

    // Load the main query class and run it.
    val queryClass = s"raw.query.$queryName"
    val clazz = rawClassloader.loadClass(queryClass)

    val ctorTypeArgs = List.fill(accessPaths.size)(classOf[RDD[_]])
    val ctor = clazz.getConstructor(ctorTypeArgs: _*)
    logger.info("Invoking constructor: " + ctor)

    val ctorArgs = accessPaths.map(ap => ap.path)
    val rawQuery: RawQuery = ctor.newInstance(ctorArgs: _*).asInstanceOf[RawQuery]

    logger.info("Running query")
    Right(rawQuery.computeResult)
  }


  //  def runInterpreter(code: String, authors: AnyRef, publications: AnyRef) = {
  //    val interpreter = new ScriptEngineManager().getEngineByName("scala")
  //    val sCompiler = interpreter.asInstanceOf[IMain]
  //    //    sCompiler.settings.embeddedDefaults[String]
  //    //    -Xpluginsdir .
  //    val ret = sCompiler.settings.processArgumentString(
  //      "-Xplugin:paradise_2.11.7-2.1.0-M5.jar -Xplugin-require:macroparadise")
  //    println("ProcessArgs " + ret)
  //    println("Settings " + sCompiler.settings)
  //
  //    //    println("Classpath: " +    sCompiler.settings.classpath.value.replace(";", "\n"))
  //    //    sCompiler.settings.verbose.tryToSet(List("true"))
  //    //    sCompiler.settings.plugin.tryToSet(List("paradise_2.11.7-2.1.0-M5.jar"))
  //
  //    //    println("User settings: "+sCompiler.settings.userSetSettings.mkString("\n"))
  //    //    println("User settings: "+sCompiler.settings.visibleSettings.mkString("\n"))
  //
  //    //    scala.settings.classpath.tryToSet(List(classpath))
  //    //    println(scala.settings)
  //
  //    //    scala.eval("println(\"Hello\")")
  //    //    println(ret)
  //
  //    //    println("compiling code:")
  //
  //    val urlCL = RDD.getClass.getClassLoader.asInstanceOf[URLClassLoader]
  //    println("URLs: " + urlCL.getURLs.mkString("\n"))
  //    println("authors classloader: " + RDD.getClass.getClassLoader)
  //
  //
  //    //    val cl: scala.reflect.internal.util.AbstractFileClassLoader = null
  //    //    cl.getParent
  //    val b = sCompiler.getBindings(ScriptContext.ENGINE_SCOPE)
  //    b.put("authors", authors)
  //    b.put("publications", publications)
  //
  //    val compiled: CompiledScript = sCompiler.compile(code)
  //    println("Compiled: " + compiled)
  //
  //    val evaled: AnyRef = compiled.eval(b)
  //    println("evaled: " + evaled)
  //  }


  //  def compileToJar(code: String): Path = {
  //    val file: Path = Files.createTempFile("query-code", ".scala")
  //    Files.write(file, code.getBytes(StandardCharsets.UTF_8))
  //    logger.info(s"Wrote source file: ${file.toAbsolutePath.toString}")
  //
  //    new compiler.Run().compile(List(file.toString))
  //
  //    val jarFile: Path = Files.createTempFile("query-", ".jar")
  //    println("Writing file " + jarFile)
  //    ZipUtil.pack(classOutputDir.toFile, jarFile.toFile)
  //    jarFile
  //  }
  //
  //  private[this] def loadClass(jarFile: Path, queryClass: String, authorsRDD: AnyRef, publicationsRDD: AnyRef): raw.RawQuery = {
  //    val cl = Thread.currentThread().getContextClassLoader
  //    logger.info("Context classloader " + cl)
  //
  //    val classloader = cl.asInstanceOf[RawMutableURLClassLoader]
  //    classloader.addURL(jarFile.toUri.toURL)
  //    var classLoader = new java.net.URLClassLoader(
  //      Array(jarFile.toUri.toURL),
  //      /*
  //       * need to specify parent, so we have all class instances
  //       * in current context
  //       */
  //      this.getClass.getClassLoader)
  //    Thread.currentThread().setContextClassLoader(classLoader)

  //    println("Classloader: " + classLoader)

  //    /*
  //     * please note that the suffix "$" is for Scala "object",
  //     * it's a trick
  //     */
  //    var clazzExModule = classLoader.loadClass(Module.ModuleClassName + "$")

  //    var clazz = classloader.loadClass(queryClass)
  //    println("Creating a new instance of class: " + clazz)
  //    println("Constructors: " + clazz.getConstructors.mkString("\n"))
  //    println("Methods: " + clazz.getDeclaredMethods.mkString("\n"))
  //    val ctor = clazz.getConstructor(classOf[RDD[_]], classOf[RDD[_]])
  //    ctor.newInstance(authorsRDD, publicationsRDD).asInstanceOf[RawQuery]

  //    val m = clazz.getDeclaredMethod("computeResult")
  //    println("Method: " + m)
  //    val result = m.invoke(rawQuery)
  //    println("Result: " + result)


  //    /*
  //     * currently, I don't know how to check if clazzExModule is instance of
  //     * Class[Module], because clazzExModule.isInstanceOf[Class[_]] always
  //     * returns true,
  //     * so I use try/catch
  //     */
  //    try {
  //      //"MODULE$" is a trick, and I'm not sure about "get(null)"
  //      var module = clazzExModule.getField("MODULE$").get(null).asInstanceOf[Module]
  //    } catch {
  //      case e: java.lang.ClassCastException =>
  //        printf(" - %s is not Module\n", clazzExModule)
  //    }
  //  }
}
