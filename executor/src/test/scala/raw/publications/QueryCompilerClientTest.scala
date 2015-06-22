package raw.publications

import com.google.common.collect.ImmutableMultiset
import org.apache.spark.rdd.RDD
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.LogicalAlgebraPrettyPrinter
import raw.csv.Student
import raw.{RawQuery, rawQueryAnnotation}

//@rawQueryAnnotation
//class CountAuthors(val authors: List[Author]) extends RawQuery {
//  val oql = """count(authors)"""
//}

@rawQueryAnnotation
//class Query1(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
class Query1(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  //    val oql = """select title, count(partition) as n from authors A group by title: A.title"""
  //    val oql = """select P from publications P where "particle detectors" in P.controlledterms"""
  //  val oql = """select a.title, a.name from authors a where a.title = "PhD""""
  //  val oql = """select title, (select distinct year from partition) as years from authors A group by title: A.title"""
  //  val oql = """select distinct title, (select distinct A from partition) as people from authors A group by title: A.title"""

  // joins
  //  val oql = """select a, b from authors a, authors b where a.year = b.year"""

  // joins between tables
//  val oql = """select distinct author, (select P from partition) as articles from publications P, P.authors A where P.title = "titletralala" and A = P.title group by author: A"""
  val oql = """select author, (select P from partition) as articles from publications P, P.authors A group by author: A"""
}

class QueryCompilerClientTest extends AbstractSparkPublicationsTest {
  def processPlan(plan: LogicalAlgebraNode): Unit = {
    logger.info("Plan: {}", LogicalAlgebraPrettyPrinter(plan))
  }

  //  test("simple query") {
  //    val compilerClient = new QueryCompilerClient
  //    compilerClient.compile("count(authors)") match {
  //      case Left(e) => { assert(false, "Error compiling query: " + e) }
  //      case Right(plan) => processPlan(plan)
  //    }
  //  }

  test("simple oql") {
    //    val result: ImmutableMultiset[Author] = (new Query1(authorsRDD).computeResult).asInstanceOf[ImmutableMultiset[Author]]
    val result = (new Query1(authorsRDD, publicationsRDD).computeResult).asInstanceOf[ImmutableMultiset[_]]
    logger.info("Result: " + result)
  }
}