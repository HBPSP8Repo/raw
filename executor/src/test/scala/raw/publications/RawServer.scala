package raw.publications

import scala.reflect.runtime.{universe => ru}

// TODO: WIP, trying to using the toolbox to compile the query at runtime.
class RawServerTest extends AbstractSparkPublicationsTest {

  def generateCode(queryName: String, query: String) = {
    s"""
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._

@rawQueryAnnotation
class ${queryName}(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = \"\"\"
    s"$query"
    \"\"\"
  def computeResult = 0
}
"""
}

//  test("Compiler") {
//    val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

//    val q: () => Any = RawCompileTime.query("count(authors)", authorsRDD)
//    logger.info(s"Query: $q")
//    val query = q()
//    logger.info(s"Result: $query")
//
//    RawCompileTime.invoke(query.asInstanceOf[AnyRef], authorsRDD)

//    val classText= generateCode("FooBarQuery", "count(authors)")
//    println(s"Code: $classText")
//
//    val parsedTree = tb.parse(classText)
//    println(s"Parsed: $parsedTree")

//    val compiled = tb.compile(parsedTree)
//    println(s"Compiled: $compiled")
//  }
}
