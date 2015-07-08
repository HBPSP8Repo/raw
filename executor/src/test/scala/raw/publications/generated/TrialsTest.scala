//package raw.publications.generated
//import org.apache.spark.rdd.RDD
//import raw.{rawQueryAnnotation, RawQuery}
//import raw.publications._
//import com.google.common.collect.ImmutableMultiset
//import scala.collection.JavaConversions
//
//
//@rawQueryAnnotation
//class Trials0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
//  val oql = """
//select * from (
//    select article: P,
//           (select A
//            from P.authors a,
//                 authors A
//            where A.name = a
//                  and A.title = "professor") as profs
//    from publications P
//    where "particle detectors" in P.controlledterms
//          and "Stricker, D.A." in P.authors
//    ) T having count(T.profs) > 0
//  """
//}
//
//
//class TrialsTest extends AbstractSparkPublicationsTest {
//
//  test("Trials0") {
//    val result = new Trials0Query(authorsRDD, publicationsRDD).computeResult
//    val actual = convertToString(result)
//    println(actual)
////    val expected = ""
////    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
//  }
//
//}
