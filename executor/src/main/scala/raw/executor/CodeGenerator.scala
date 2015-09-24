package raw.executor

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext

/**
 * Interface implemented by classes generated at runtime to load Scala access paths.
 */
trait RawScalaLoader {
  def loadRawScanner(rawSchema: RawSchema): RawScanner[_]
}

// TODO: implement the Spark access path loader
///**
// * Interface implemented by classes generated at runtime to load Spark access paths
// */
//trait SparkLoader {
//  def loadAccessPaths(rawSchema: RawSchema, sc: SparkContext): AccessPath[_]
//}

object CodeGenerator extends StrictLogging with ResultConverter {
  val rawClassloader = new RawMutableURLClassLoader(getClass.getClassLoader)
  private[this] val queryCompiler = new RawCompiler(rawClassloader)
  private[this] val ai = new AtomicInteger(0)

  private[this] def extractInnerType(parametrizedType: String): String = {
    val start = parametrizedType.indexOf("[") + 1
    val end = parametrizedType.length - 1
    parametrizedType.substring(start, end)
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

  def query(logicalPlan: String, queryPaths: Seq[RawScanner[_]]): String = {
    val query = queryCompiler.compileLogicalPlan(logicalPlan, queryPaths)
    val result = query.computeResult
    convertToJson(result)
  }

  def loadScanner(name: String, schema: RawSchema, sc: SparkContext = null): RawScanner[_] = {
    val parsedSchema: ParsedSchema = SchemaParser(schema)

    val innerType = extractInnerType(parsedSchema.typeDeclaration)
    val caseClassesSource = parsedSchema.caseClasses.values.mkString("\n")
    val loaderClassName = s"Loader${ai.getAndIncrement()}__${name}"

    if (sc == null) {
      // Scala local executor
      val sourceCode = genScalaLoader(caseClassesSource, loaderClassName, innerType, name)
      val scalaLoader = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[RawScalaLoader]
      scalaLoader.loadRawScanner(schema)

    } else {
      ???
    }
  }

  // TODO: Reduce the code generation section. Define only the case classes and return all the type information as
  // typetag, classtag and manifest.
  private[this] def genScalaLoader(caseClassesSource: String, loaderClassName: String, innerType: String, name: String): String = {
    s"""
package raw.query

import java.nio.file.Path
import raw.executor._

${caseClassesSource}

class ${loaderClassName} extends RawScalaLoader {
  override def loadRawScanner(rawSchema: RawSchema): RawScanner[_] = {
    rawSchema.fileType match {
      case "json" => new JsonRawScanner(rawSchema, manifest[$innerType])
      case "csv" => new CsvRawScanner(rawSchema, manifest[$innerType])
      case a @ _ => throw new Exception("Unknown file type: " + a)
    }
  }
}
"""
  }
}
