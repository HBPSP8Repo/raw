//package raw.publications.generated
//import org.apache.spark.rdd.RDD
//import raw.{rawQueryAnnotation, RawQuery}
//import raw.datasets.publications._
//import raw.publications._
//
//
//@rawQueryAnnotation
//class Complex0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
//  val oql = """
//    select * from (select article.title, min(article.phd) as mphd, max(article.profs) as mprofs, count(article.phd) as cphd, count(article.profs) as cprofs from (
//        select doc.p.title as title,
//            (select p.year from doc.people p where p.title = "PhD") as phd,
//            (select p.year from doc.people p where p.title = "professor") as profs
//                from (
//                    select p, (select A from p.authors a, authors A where A.name = a) as people
//                    from publications p
//                    ) doc
//                    ) article) X where X.cphd > 0 and X.cprofs > 0 and X.mphd < X.mprofs
//  """
//}
//
//
//class ComplexTest extends AbstractSparkPublicationsTest {
//
//  test("Complex0") {
//    val result = new Complex0Query(authorsRDD, publicationsRDD).computeResult
//    val actual = convertToString(result)
//
//    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
//  }
//
//}
