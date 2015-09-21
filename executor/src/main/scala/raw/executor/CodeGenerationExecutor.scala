package raw.executor

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.datasets.AccessPath
import raw.utils.Instrumented

import scala.collection.mutable

/**
 * Interface implemented by classes generated at runtime to load Scala access paths.
 */
trait ScalaLoader {
  def loadAccessPaths(rawSchema: RawSchema): AccessPath[_]
}

/**
 * Interface implemented by classes generated at runtime to load Spark access paths
 */
trait SparkLoader {
  def loadAccessPaths(rawSchema: RawSchema, sc: SparkContext): AccessPath[_]
}

object CodeGenerationExecutor extends StrictLogging with ResultConverter {
  val rawClassloader = new RawMutableURLClassLoader(getClass.getClassLoader)
  private[this] val queryCompiler = new QueryCompilerClient(rawClassloader)
  private[this] val ai = new AtomicInteger(0)
  private[this] val accessPaths = new mutable.HashMap[String, AccessPath[_]]()

  private[this] def extractInnerType(parametrizedType: String): String = {
    val start = parametrizedType.indexOf("[") + 1
    val end = parametrizedType.size - 1
    parametrizedType.substring(start, end)
  }

  //  /**
  //   *
  //   * @param name The name of the access path. This is the identifier used in the logical plan and in the
  //   *             code-generated query implementation.
  //   * @param schemaXML The XML representing the schema as generated by ldb:wn/wn/schema_serializer.py
  //   * @param fileLocation The location of the file containing the data (must be local).
  //   * @param sc If non-null, generates a Spark access path with the given SparkContext. If null, create a Scala access path.
  //   */
  //  def registerAccessPath(name: String, schemaXML: Reader, fileLocation: Path, sc: SparkContext = null): Unit = {
  //    val parsedSchema = SchemaParser(schemaXML)
  //
  //    val innerType = extractInnerType(parsedSchema.typeDeclaration)
  //    val caseClassesSource = parsedSchema.caseClasses.values.mkString("\n")
  //    val loaderClassName = s"Loader${ai.getAndIncrement()}__${name}"
  //
  //    val accessPath = if (sc == null) {
  //      // Scala local executor
  //      val sourceCode = genScalaLoader(caseClassesSource, loaderClassName, innerType, name)
  //      val scalaLoader = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[ScalaLoader]
  //      scalaLoader.loadAccessPaths(fileLocation)
  //
  //    } else {
  //      // Spark distributed executor
  //      val sourceCode = genSparkLoader(caseClassesSource, loaderClassName, innerType, name)
  //      val sparkLoader = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[SparkLoader]
  //      sparkLoader.loadAccessPaths(fileLocation, sc)
  //    }
  //
  //    accessPaths.put(name, accessPath)
  //  }

  def loadAccessPath(name: String, schema: RawSchema, sc: SparkContext = null): AccessPath[_] = {
    val parsedSchema: ParsedSchema = SchemaParser(schema)

    val innerType = extractInnerType(parsedSchema.typeDeclaration)
    val caseClassesSource = parsedSchema.caseClasses.values.mkString("\n")
    val loaderClassName = s"Loader${ai.getAndIncrement()}__${name}"

    if (sc == null) {
      // Scala local executor
      val sourceCode = genScalaLoader(caseClassesSource, loaderClassName, innerType, name)
      val scalaLoader = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[ScalaLoader]
      scalaLoader.loadAccessPaths(schema)

    } else {
      // Spark distributed executor
      val sourceCode = genSparkLoader(caseClassesSource, loaderClassName, innerType, name)
      val sparkLoader = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[SparkLoader]
      sparkLoader.loadAccessPaths(schema, sc)
    }
  }

  private[this] def genScalaLoader(caseClassesSource: String, loaderClassName: String, innerType: String, name: String): String = {
    s"""
package raw.query

import java.nio.file.Path
import raw.datasets.AccessPath
import raw.executor.{Loader, ScalaLoader, RawSchema}

${caseClassesSource}

class ${loaderClassName} extends ScalaLoader {

  def loadAccessPaths(rawSchema: RawSchema): AccessPath[_] = {
    val data = Loader.loadAbsolute[List[${innerType}]](rawSchema)
    AccessPath("$name", Left(data))
  }
}
"""
  }

  private[this] def genSparkLoader(caseClassesSource: String, loaderClassName: String, innerType: String, name: String): String = {
    s"""
package raw.query

import java.nio.file.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.datasets.AccessPath
import raw.executor.{JsonLoader, SparkLoader, RawSchema}
import raw.spark.DefaultSparkConfiguration

${caseClassesSource}

class ${loaderClassName} extends SparkLoader {
  def loadAccessPaths(rawSchema: RawSchema, sc: SparkContext): AccessPath[_] = {
    val data = JsonLoader.loadAbsolute[List[${innerType}]](rawSchema.dataFile)
    val rdd = DefaultSparkConfiguration.newRDDFromJSON(data, rawSchema.properties, sc)
    AccessPath("$name", Right(rdd))
  }
}
"""
  }

  /**
   * Execute a query expressed as a logical plan. The query can use all of the schemas previously registered with
   * registerAccessPath()
   *
   * @param logicalPlan
   * @return A JSON representing the result.
   */
  def query(logicalPlan: String): String = {
    val queryPaths: Seq[AccessPath[_]] = accessPaths.values.toSeq
    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
    val result = query.computeResult
    convertToJson(result)
  }

  def query(logicalPlan: String, queryPaths: Seq[AccessPath[_]]): String = {
    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
    val result = query.computeResult
    convertToJson(result)
  }

  //  def query(logicalPlan: String, paths: Seq[String]): String = {
  //    logger.info("AccessPaths: " + paths)
  //    val queryPaths: Seq[AccessPath[_]] = paths.map(p => accessPaths(p))
  //
  //    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
  //    val result = query.computeResult
  //    convertToJson(result)
  //  }
}
