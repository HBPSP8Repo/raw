package raw.executor

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.QueryLanguages.QueryLanguage
import raw.RawQuery

import scala.reflect._
import scala.reflect.runtime.universe._


/**
 * Carrier for type information of the case classes generated at runtime. The ClassTag, TypeTag and Manifest stored in
 * instances of this class will be filled by the compiler which is invoked at runtime to generate the case classes.
 *
 * This type information will then be available to any other types whose instances are created from within the scope
 * of this class, for instance, the RawScanner.
 *
 * NOTE: for some reason, it is not possible to create a RawScanner outside the scope of this class by giving it
 * explicilty the type arguments kept by this class. The type informaiton is erased and replaced by "_$X"
 *
 * @tparam T The top level type of the schema created at runtime.
 */
class SchemaTypeInformation[T: ClassTag : TypeTag : Manifest] {
  def createScanner(schema: RawSchema): RawScanner[T] = {
    RawScanner(schema)
  }
}

abstract class SchemaTypeFactory {
  def getSchemaInformation: SchemaTypeInformation[_]
}

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

object CodeGenerator extends StrictLogging {

  import ResultConverter._

  val rawClassloader = new RawMutableURLClassLoader(getClass.getClassLoader)
  private[this] val queryCompiler = new RawCompiler(rawClassloader)
  private[this] val ai = new AtomicInteger(0)

  private[this] def extractInnerType(parametrizedType: String): String = {
    val start = parametrizedType.indexOf("[") + 1
    val end = parametrizedType.length - 1
    parametrizedType.substring(start, end)
  }

  //  private[this] def genSparkLoader(caseClassesSource: String, loaderClassName: String, innerType: String, name: String): String = {
  //    s"""
  //package raw.query
  //
  //import java.nio.file.Path
  //import org.apache.spark.SparkContext
  //import org.apache.spark.rdd.RDD
  //import raw.datasets.AccessPath
  //import raw.executor.{JsonLoader, SparkLoader, RawSchema}
  //import raw.spark.DefaultSparkConfiguration
  //
  //${caseClassesSource}
  //
  //class ${loaderClassName} extends SparkLoader {
  //  def loadAccessPaths(rawSchema: RawSchema, sc: SparkContext): AccessPath[_] = {
  //    val data = JsonLoader.loadAbsolute[List[${innerType}]](rawSchema.dataFile)
  //    val rdd = DefaultSparkConfiguration.newRDDFromJSON(data, rawSchema.properties, sc)
  //    AccessPath("$name", Right(rdd))
  //  }
  //}
  //"""
  //  }

  def compileQuery(queryLanguage: QueryLanguage, logicalPlan: String, queryPaths: Seq[RawScanner[_]]): RawQuery = {
    queryCompiler.compile(queryLanguage, logicalPlan, queryPaths)
  }

  def query(queryLanguage: QueryLanguage, logicalPlan: String, queryPaths: Seq[RawScanner[_]]): String = {
    val query = queryCompiler.compile(queryLanguage, logicalPlan, queryPaths)
    val result = query.iterator
    try {
      convertToJson(result)
    } finally {
      result.close()
    }
  }

  def loadScanner(name: String, schema: RawSchema, sc: SparkContext = null): RawScanner[_] = {
    val parsedSchema: ParsedSchema = SchemaParser(schema)

    val innerType = extractInnerType(parsedSchema.typeDeclaration)
    val caseClassesSource = parsedSchema.caseClasses.values.mkString("\n")
    val loaderClassName = s"Loader${ai.getAndIncrement()}__${name}"

    if (sc == null) {
      val sourceCode = genCaseClasses(caseClassesSource, loaderClassName, innerType)
      val scalaLoader: SchemaTypeFactory = queryCompiler.compileLoader(sourceCode, loaderClassName).asInstanceOf[SchemaTypeFactory]
      scalaLoader.getSchemaInformation.createScanner(schema)
    } else {
      ???
    }
  }


  private[this] def genCaseClasses(caseClassesSource: String, loaderClassName: String, innerType: String): String = {
    s"""package raw.query

import scala.reflect._
import scala.reflect.runtime.universe._
import raw.executor._

${caseClassesSource}

class ${loaderClassName} extends SchemaTypeFactory {
  override def getSchemaInformation: SchemaTypeInformation[_] = {
    new SchemaTypeInformation[$innerType]
  }
}"""
  }
}
