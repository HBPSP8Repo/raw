package raw.publications

import java.lang.reflect.Field

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMultiset
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

  def convertActual(res: Any): String = {
    res match {
      case imSet: ImmutableMultiset[_] =>
        val resultTyped = toScalaList(imSet)
        resultsToString(resultTyped)
      case set: Set[_] =>
        resultsToString(set)
      case list: List[_] =>
        resultsToString(list)
      case _ => res.toString
    }
  }

  def toScalaList[T](s: ImmutableMultiset[T]) = {
    JavaConversions.asScalaIterator[T](s.iterator).toList
  }

  //
  //  def resultsToString[T](s: ImmutableMultiset[T]): String = {
  //    val list = JavaConversions.asScalaIterator[T](s.iterator).toList
  //    resultsToString(list)
  //  }

  def resultsToString[T](s: Set[T]): String = {
    resultsToString(s.toList)
  }

  def resultsToString[T](l: List[T]): String = {
    l.map(valueToString(_)).sorted.mkString("\n")
  }

  def valueToString[T](value: Any): String = {
    value match {
      //      case a: Author => valueToString(List(s"name: ${a.name}", s"title: ${a.title}", s"year: ${a.year}"))
      //      case p: Publication => valueToString(List(
      //        s"title: ${p.title}",
      //        s"authors: ${valueToString(p.authors)}",
      //        s"affiliations: ${valueToString(p.affiliations)}",
      //        s"controlledterms: ${valueToString(p.controlledterms)}"))
      case l: List[_] => l.map(valueToString(_)).sorted.mkString("[", ", ", "]")
      case s: Set[_] => s.map(valueToString(_)).toList.sorted.mkString("[", ", ", "]")
      case m: Map[_, _] => m.map({ case (k, v) => s"$k: ${valueToString(v)}" }).toList.sorted.mkString("[", ", ", "]")
      case p: Product => valueToString(caseClassToMap(p))
      case _ => value.toString
    }
  }

  def convertExpected(expected: String): String = {
    expected.replaceAll("""\n\s*""", "\n").trim
  }

  // WARN: There are many classes that implement Product, like List. If this method is called with something other
  // than a case class, it will likely fail.
  def caseClassToMap(p: Product): Any = {
    val fields: Array[Field] = p.getClass.getDeclaredFields
    fields.map(f => {
      f.setAccessible(true)
      f.getName -> valueToString(f.get(p))
    }
    ).toMap
  }
}
