package raw.publications

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.SharedSparkContext
import raw.executionserver.{ResultConverter, ScalaDataSet}

import scala.reflect.ClassTag

abstract class AbstractSparkPublicationsTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter {
  var authorsRDD: RDD[Author] = _
  var publicationsRDD: RDD[Publication] = _

  def newRDDFromJSON[T](lines: List[T], sparkContext: SparkContext)(implicit ct: ClassTag[T]) = {
    val start = Stopwatch.createStarted()
    val rdd: RDD[T] = sparkContext.parallelize(lines)
    logger.info("Created RDD. Partitions: " + rdd.partitions.map(p => p.index).mkString(", ") + ", partitioner: " + rdd.partitioner)
    rdd
  }

  override def beforeAll() {
    super.beforeAll()
    authorsRDD = newRDDFromJSON[Author](ScalaDataSet.authors, sc)
    publicationsRDD = newRDDFromJSON[Publication](ScalaDataSet.publications, sc)
  }
}
