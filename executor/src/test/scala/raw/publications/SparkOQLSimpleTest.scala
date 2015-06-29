package raw.publications

import org.apache.spark.rdd.RDD
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.LogicalAlgebraPrettyPrinter
import raw.{RawQuery, rawQueryAnnotation}


@rawQueryAnnotation
//class Query1(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
class Query1(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  //  val oql = """select distinct title, (select distinct A from partition) as people from authors A group by title: A.title"""

  // joins
    //  val oql = """select a, b from authors a, authors b where a.year = b.year"""

  // joins between tables
  //  val oql = """select distinct author, (select P from partition) as articles from publications P, P.authors A where P.title = "titletralala" and A = P.title group by author: A"""
  val oql = """select author, (select P from partition) as articles from publications P,   P.authors A group by author: A"""
}

