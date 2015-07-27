package raw.datasets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.executionserver.{DefaultSparkConfiguration, JsonLoader}

import scala.reflect._

/**
 * If the type parameter used in the access path contains custom subtypes, they must be in the same package as the top
 * level types. This is because the code generator will only add imports for the packages of the top-level types.
 *
 * @param name The identifier of the access path, used to generate the query code.
 * @param path The actual instance of the access path, used to execute the query.
 * @param tag Type information of the access path. Used to generate the query code.
 */
// Note: name and tag are execute for generating the code, while path is for creating an instance of the query.
// The compilation and instantiation should be separated. This would allow caching the queries and executing the
// same query with different access paths.
case class AccessPath[T: ClassTag](name: String, path: RDD[T])(implicit val tag: ClassTag[T])

class Dataset[T: ClassTag](name: String, file: String, sc: SparkContext) {
  val data: List[T] = JsonLoader.load[T](file)
  val rdd = DefaultSparkConfiguration.newRDDFromJSON[T](data, sc)
  val accessPath: AccessPath[_] = AccessPath[T](name, rdd)
}

