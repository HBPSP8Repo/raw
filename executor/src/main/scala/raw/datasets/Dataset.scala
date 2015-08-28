package raw.datasets


import java.nio.file.Paths

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
 * @param elemTag Type information of the access path. Used to generate the query code.
 */
// Note: name and tag are used for generating the code, while path is for creating an instance of the query.
// The compilation and instantiation should be separated. This would allow caching the queries and executing the
// same query with different access paths.
//case class SparkAccessPath[T <: Product](name: String, path: RDD[T], tag: TypeTag[T])
//
//case class ScalaAccessPath[T <: Product](name: String, path: List[T], tag: TypeTag[T])

//case class FunkyAccessPath[T<: Product, PathType[T]](name: String, path: PathType[T])(implicit tag:TypeTag[PathType])

case class AccessPath[T](name: String, path: Either[List[T], RDD[T]])(implicit val tag: TypeTag[T])

case class Dataset[T <: Product](name: String, file: String, tag: TypeTag[T])

object AccessPath {
  def toRDDAcessPath[T <: Product : ClassTag : TypeTag](dataset: Dataset[T], sc: SparkContext): AccessPath[T] = {
    val data = JsonLoader.load[List[T]](dataset.file)
    val rdd = DefaultSparkConfiguration.newRDDFromJSON(data, sc)
    new AccessPath[T](dataset.name, Right(rdd))
  }

  def toScalaAcessPath[T <: Product : ClassTag : TypeTag](dataset: Dataset[T]): AccessPath[T] = {
    val data: List[T] = JsonLoader.load[List[T]](dataset.file)
    new AccessPath[T](dataset.name, Left(data))
  }

  def toScalaAcessPathAbs[T <: Product : ClassTag : TypeTag](dataset: Dataset[T]): AccessPath[T] = {
//    val t = implicitly[TypeTag[T]]
//    val data: List[T] = JsonLoader.loadAbsolute(Paths.get(dataset.file))(typeTag[List[T]])
    val data: List[T] = JsonLoader.loadAbsolute(Paths.get(dataset.file))(manifest[List[T]])
    new AccessPath[T](dataset.name, Left(data))
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

  def loadScalaDataset[_ <: Product](dsName: String): List[AccessPath[_ <: Product]] = {
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
