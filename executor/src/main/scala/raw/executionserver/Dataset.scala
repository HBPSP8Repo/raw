package raw.executionserver

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class AccessPath[T](name: String, path: RDD[T], tag: ClassTag[T])

abstract class Dataset {
  val accessPaths: List[AccessPath[_]]
}

