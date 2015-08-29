package raw.executionserver

import java.io.Reader
import java.net.URL
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import raw.datasets.AccessPath
import raw.perf.QueryCompilerClient

import scala.collection.mutable

/**
 * Interface implemented by classes generated to load the datasets based on a given schema.
 */
trait Loader {
  def loadAccessPaths(path: String): AccessPath[_]
}

object RawServer extends StrictLogging with ResultConverter {
  val rawClassloader = new RawMutableURLClassLoader(new Array[URL](0), getClass.getClassLoader)
  val queryCompiler = new QueryCompilerClient(rawClassloader)
  val ai = new AtomicInteger(0)

  def extractInnerType(parametrizedType: String): String = {
    val start = parametrizedType.indexOf("[") + 1
    val end = parametrizedType.size - 1
    parametrizedType.substring(start, end)
  }

  private[this] val accessPaths = new mutable.HashMap[String, AccessPath[_]]()

  def registerSchema(name: String, schemaXML: Reader, fileLocation: String): Unit = {
    val parsedSchema = SchemaParser(schemaXML)

    val innerType = extractInnerType(parsedSchema.typeDeclaration)
    val caseClassesSource = parsedSchema.caseClasses.values.mkString("\n")
    val loaderClassName = s"Loader${ai.getAndIncrement()}__${name}"

    val sourceCode = s"""
      package raw.query

      import java.nio.file.Paths
      import raw.datasets.AccessPath
      import raw.executionserver.{JsonLoader, Loader}

      ${caseClassesSource}

      class ${loaderClassName} extends Loader {
        def loadAccessPaths(path: String): AccessPath[_] = {
          val listManifest = manifest[List[${innerType}]]
          val p = Paths.get(path)
          val data = JsonLoader.loadAbsolute(p)(listManifest)
          AccessPath("$name", Left(data))
        }
      }
    """

    val obj = queryCompiler.compileLoader(sourceCode, loaderClassName)
    val accessPath = obj.loadAccessPaths(fileLocation)
    accessPaths.put(name, accessPath)
  }

  def query(logicalPlan: String): String = {
    val queryPaths: Seq[AccessPath[_]] = accessPaths.values.toSeq
    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
    val result = query.computeResult
    convertToJson(result)
  }

  def query(logicalPlan: String, paths: Seq[String]): String = {
    logger.info("AccessPaths: " + paths)
    val queryPaths: Seq[AccessPath[_]] = paths.map(p => accessPaths(p))

    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
    val result = query.computeResult
    convertToJson(result)
  }
}
