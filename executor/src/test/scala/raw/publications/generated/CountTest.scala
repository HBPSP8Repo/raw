package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._


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

    val authorsCount = result.asInstanceOf[Int]
    assert(authorsCount === 50, "Wrong number of authors")
  }

  test("Count1") {
    val result = new Count1Query(authorsRDD, publicationsRDD).computeResult

    val pubsCount = result.asInstanceOf[Int]
    assert(pubsCount === 1000, "Wrong number of publications")
  }

}
