package raw
package util

import java.net.URL

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class CSVParserError(err: String) extends RawException(err)

object CSVParser {
  def apply[T](path: String, parse: (List[String] => T), delim: String = ",", skipLines: Int = 0): List[T] = {
    val content = scala.io.Source.fromURL(Resources.getResource(path))
    content.getLines()
      .drop(skipLines)
      .map(_.split(delim).toList)
      .map(parse)
      .toList
  }
}

class CSVToRDDParser(sc: SparkContext) extends StrictLogging {
  /** Concerning the T:ClassTag implicit parameter:
    * http://apache-spark-user-list.1001560.n3.nabble.com/Generic-types-and-pair-RDDs-td3593.html
    */
  def parse[T: ClassTag](filePath: String, parse: (List[String] => T), delim: String = ","): RDD[T] = {
    val url: URL = Resources.getResource(filePath)
    sc.textFile(url.getPath())
      .map(_.split(delim).toList)
      .map(parse)
  }
}