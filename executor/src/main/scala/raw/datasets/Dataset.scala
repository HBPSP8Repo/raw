package raw.datasets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import raw.executionserver.{DefaultSparkConfiguration, JsonLoader}

import scala.reflect._

case class AccessPath[T](name: String, path: RDD[T], tag: ClassTag[T])

class Dataset[T:ClassTag](name:String, file:String, sc:SparkContext) {
  val data: List[T] = JsonLoader.load[T](file)
  val rdd = DefaultSparkConfiguration.newRDDFromJSON[T](data, sc)
  val accessPath: AccessPath[_] = AccessPath(name, rdd, classTag[T])
}

