//package raw.publications.generated
//
//import com.google.common.collect.ImmutableMultiset
//import org.apache.spark.rdd.RDD
//import raw.{rawQueryAnnotation, RawQuery}
//import raw.datasets.publications._
//import raw.publications._
//
//
//@rawQueryAnnotation
//class Assign0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
//  val oql = """
//    select G.title,
//          (select year: v,
//                  N: count(partition)
//           from v in G.values
//           group by year: v) as stats
//    from (
//          select distinct title, (select year from partition) as values
//          from authors A
//          group by title: A.title) G
//  """
//}
//
//
//class AssignTest extends AbstractSparkPublicationsTest {
//
//  test("Assign0") {
//    val result = new Assign0Query(authorsRDD, publicationsRDD).computeResult
//    val actual = convertToString(result)
//  }
//
//}
