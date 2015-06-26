package raw.publications

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.SharedSparkContext

import scala.collection.JavaConversions
import scala.reflect.ClassTag

abstract class AbstractSparkPublicationsTest extends FunSuite with StrictLogging with BeforeAndAfterAll with SharedSparkContext {
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

  def listToString[T](list:Seq[T]) = {
    list.mkString(", ")
  }

  def authorToString(a:Author) : String = {
    s"${a.name}; ${a.title}; ${a.year}"
  }

  def pubToString(p:Publication) : String = {
    s"${p.title}; ${listToString(p.authors)}; ${listToString(p.affiliations)}; ${listToString(p.controlledterms)}"
  }

  def convert[T](iterator: java.util.Iterator[T], toStringF:(T => String)): String = {
    convertInner(JavaConversions.asScalaIterator[T](iterator), toStringF)
  }

  //  def convert[T](map: Map[T]): String = {
  //    convert(map.toIterator, Map.toString)
  //  }

  def convertInner[T](iter: Iterator[T], toStringF:(T => String)): String = {
    iter.map(toStringF(_))
      .toList
      .sorted
      .mkString("\n").trim
  }

  /* Produces one line of output for each List[String] in s
   */
  def toString(s: Set[List[String]]):String = {
    s.map(_.sorted.mkString("; "))
      .toList
      .sorted
      .mkString("\n")
  }


  def convertExpected(expected: String): String = {
    expected.replaceAll("""\n\s*""", "\n").trim
  }
}
