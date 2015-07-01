package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._
import com.google.common.collect.ImmutableMultiset
import scala.collection.JavaConversions


@rawQueryAnnotation
class Trials0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select author as a1, (select distinct title as t1, affiliations as aff from partition) as articles
    from publications P, P.authors A
    where A = "Akoh, H."
    group by author: A
  """
}


class TrialsTest extends AbstractSparkPublicationsTest {

  test("Trials0") {
    val result = new Trials0Query(authorsRDD, publicationsRDD).computeResult
    val actual = convertActual(result)

    logger.info(actual)
//    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
