package raw.datasets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.datasets.patients.Patients
import raw.datasets.publications.Publications
import raw.executionserver.{DefaultSparkConfiguration, JsonLoader}

import scala.reflect._
import scala.reflect.runtime.universe._

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
case class AccessPath[T <: Product](name: String, path: RDD[T], tag: TypeTag[T])

object AccessPath {
  def loadJSON[T <: Product : ClassTag : TypeTag](name: String, file: String, sc: SparkContext): AccessPath[T] = {
    val data: List[T] = JsonLoader.load[List[T]](file)
    val rdd = DefaultSparkConfiguration.newRDDFromJSON[T](data, sc)
    val accessPath: AccessPath[T] = new AccessPath[T](name, rdd, typeTag[T])
    accessPath
  }

  def loadDataset(dsName: String, sc: SparkContext): List[AccessPath[_ <: Product]] = {
    dsName match {
      case "publications" => Publications.publications(sc)
      case "publicationsSmall" => Publications.publicationsSmall(sc)
      case "publicationsSmallDups" => Publications.publicationsSmallDups(sc)
      case "publicationsLarge" => Publications.publicationsLarge(sc)
      case "patients" => Patients.loadPatients(sc)
      case _ => throw new IllegalArgumentException(s"Invalid dataset: $dsName. Valid options: publications, publicationsSmallDups, publicationsLarge, patients.")
    }
  }
}
