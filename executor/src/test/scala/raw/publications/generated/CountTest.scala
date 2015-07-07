package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._
import com.google.common.collect.ImmutableMultiset
import scala.collection.JavaConversions


@rawQueryAnnotation
class Count0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    count(authors)
  """
}

@rawQueryAnnotation
class Count1Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    count(publications)
  """
}


class CountTest extends AbstractSparkPublicationsTest {

  test("Count0") {
    val result = new Count0Query(authorsRDD, publicationsRDD).computeResult
    val actual = convertToString(result)
    val expected = "50"
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Count1") {
    val result = new Count1Query(authorsRDD, publicationsRDD).computeResult
    val actual = convertToString(result)
    val expected = "1000"
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
