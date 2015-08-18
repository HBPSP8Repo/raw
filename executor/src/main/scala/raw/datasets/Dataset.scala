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

case class ScalaAccessPath[T <: Product](name: String, path: List[T], tag: TypeTag[T])

case class Dataset[T <: Product](name: String, file: String, tag: TypeTag[T])


object AccessPath {
  def toRDDAcessPath[T <: Product : ClassTag : TypeTag](dataset: Dataset[T], sc: SparkContext): AccessPath[T] = {
    val data = JsonLoader.load[List[T]](dataset.file)
    val rdd = DefaultSparkConfiguration.newRDDFromJSON[T](data, sc)
    new AccessPath[T](dataset.name, rdd, dataset.tag)
  }

  def toScalaAcessPath[T <: Product : ClassTag : TypeTag](dataset: Dataset[T]): ScalaAccessPath[T] = {
    val data = JsonLoader.load[List[T]](dataset.file)
    new ScalaAccessPath[T](dataset.name, data, dataset.tag)
  }

  def loadSparkDataset(dsName: String, sc: SparkContext): List[AccessPath[_ <: Product]] = {
    dsName match {
      case "publications" => Publications.Spark.publications(sc)
      case "publicationsSmall" => Publications.Spark.publicationsSmall(sc)
      case "publicationsSmallDups" => Publications.Spark.publicationsSmallDups(sc)
      case "publicationsLarge" => Publications.Spark.publicationsLarge(sc)
      case "patients" => Patients.Spark.patients(sc)
      case _ => throw new IllegalArgumentException(s"Invalid dataset: $dsName. Valid options: publications, publicationsSmallDups, publicationsLarge, patients.")
    }
  }

  def loadScalaDataset(dsName: String): List[ScalaAccessPath[_ <: Product]] = {
    dsName match {
      case "publications" => Publications.Scala.publications()
      case "publicationsSmall" => Publications.Scala.publicationsSmall()
      case "publicationsSmallDups" => Publications.Scala.publicationsSmallDups()
      case "publicationsLarge" => Publications.Scala.publicationsLarge()
      case "patients" => Patients.Scala.patients()
      case _ => throw new IllegalArgumentException(s"Invalid dataset: $dsName. Valid options: publications, publicationsSmallDups, publicationsLarge, patients.")
    }
  }
}
