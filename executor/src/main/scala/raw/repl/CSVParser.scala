package raw
package util

import java.net.URL

import com.google.common.io.Resources
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

case class CSVParserError(err: String) extends RawException(err)

object CSVParser {
  def apply[T](path: String, parse: (List[String] => T), delim: String = ","): List[T] = {
    val content = scala.io.Source.fromURL(Resources.getResource(path))
    content.getLines()
      .map(_.split(delim).toList)
      .map(parse)
      .toList
  }
}

class CSVToRDDParser extends AutoCloseable with LazyLogging {
  logger.info("Starting local Spark context")
  val conf = new SparkConf().setAppName("RAW Unit Tests").setMaster("local")
  val sc = new SparkContext(conf)

  /** Concerning the T:ClassTag implicit parameter:
   * http://apache-spark-user-list.1001560.n3.nabble.com/Generic-types-and-pair-RDDs-td3593.html
   */
  def parse[T: ClassTag](filePath: String, parse: (List[String] => T), delim: String = ","): RDD[T] = {
    val url: URL = Resources.getResource(filePath)
    sc.textFile(url.getPath())
      .map(_.split(delim).toList)
      .map(parse)
  }

  override def close(){
    logger.info("Stopping Spark context")
    sc.stop()
  }
}

//
//object CSVParser {
//
//  def apply(path: String, tipe: Type, delim: String = ","): List[Any] = {
//    val content = scala.io.Source.fromFile(path)
//
//    //    def parse(t: Type, item: String): Any = t match {
//    //      case IntType()    => item.toInt
//    //      case FloatType()  => item.toFloat
//    //      case BoolType()   => item.toBoolean
//    //      case StringType() => item
//    //      case _            => throw CSVParserError(s"Unexpected type: $t")
//    //    }
//
//    content.getLines().map{ case line => line.split(delim).toList }.map(parse)
//    //
//    //    t match {
//    //      case ListType(RecordType(atts)) => {
//    //        val f = (l: String) => atts.zip(l.split(delim)).map { case (a, item) => (a.idn, parse(a.tipe, item))}.toMap
//    //        // TODO: This is materializing the whole file in memory!
//    //        content.getLines().toList.map(f)
//    //      }
//    //      case _                => throw CSVParserError(s"Unexpected return type: $t")
//    //    }
//  }
//
//}
