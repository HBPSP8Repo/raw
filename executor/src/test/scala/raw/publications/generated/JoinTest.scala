package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._
import com.google.common.collect.ImmutableMultiset
import scala.collection.JavaConversions


@rawQueryAnnotation
class Join0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select distinct a.year, a.name as n1, b.name as n2
        from authors a, authors b
        where a.year = b.year and a.name != b.name and a.name < b.name and a.year > 1992
  """
}

@rawQueryAnnotation
class Join1Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select distinct a.year, struct(name:a.name, title:a.title) as p1, struct(name:b.name, title:b.title) as p2
        from authors a, authors b
        where a.year = b.year and a.name != b.name and a.name < b.name and a.year > 1992
  """
}


class JoinTest extends AbstractSparkPublicationsTest {

  test("Join0") {
    val result = new Join0Query(authorsRDD, publicationsRDD).computeResult
    val actual = convertActual(result)
    
    val expected = convertExpected("""
    [n1: Johnson, R.T., n2: Martoff, C.J., year: 1994]
    [n1: Johnson, R.T., n2: Nakagawa, H., year: 1994]
    [n1: Martoff, C.J., n2: Nakagawa, H., year: 1994]
    [n1: Wang, Hairong, n2: Zhuangde Jiang, year: 1993]
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Join1") {
    val result = new Join1Query(authorsRDD, publicationsRDD).computeResult
    val actual = convertActual(result)
    
    val expected = convertExpected("""
    [p1: [name: Johnson, R.T., title: professor], p2: [name: Martoff, C.J., title: assistant professor], year: 1994]
    [p1: [name: Johnson, R.T., title: professor], p2: [name: Nakagawa, H., title: assistant professor], year: 1994]
    [p1: [name: Martoff, C.J., title: assistant professor], p2: [name: Nakagawa, H., title: assistant professor], year: 1994]
    [p1: [name: Wang, Hairong, title: professor], p2: [name: Zhuangde Jiang, title: professor], year: 1993]
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
