package raw
package util

import java.net.URL

import com.google.common.io.Resources
import com.typesafe.scalalogging.{StrictLogging, LazyLogging}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import raw.repl.RawSparkContext

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

class CSVToRDDParser extends StrictLogging {
  /* A simple form of dependency injection, inspired in the Cake Pattern
   * In this case, it would be simpler to mixin this trait directly in this class, since there is
   * a single implementation of RawSparkContext. This would be more useful if there were several
   * implementations, because then the clients using this class could choose which to use
   * (eg., production versus testing)
   */
  this : RawSparkContext =>

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