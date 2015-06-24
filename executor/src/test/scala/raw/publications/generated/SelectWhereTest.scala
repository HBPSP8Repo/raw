package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._


@rawQueryAnnotation
class SelectWhere0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select a from authors a where a.title = "PhD"
  """
}


class SelectWhereTest extends AbstractSparkPublicationsTest {

  test("SelectWhere0") {
    val result = new SelectWhere0Query(authorsRDD, publicationsRDD).computeResult

    println("Result: " + result)
  }

}
