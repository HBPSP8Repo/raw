package raw.publications.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.publications._
import com.google.common.collect.ImmutableMultiset
import scala.collection.JavaConversions


@rawQueryAnnotation
class Select0Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select distinct a.name, a.title, a.year from authors a where a.year = 1973
  """
}

@rawQueryAnnotation
class Select1Query(val authors: RDD[Author], val publications: RDD[Publication]) extends RawQuery {
  val oql = """
    select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
  """
}


class SelectTest extends AbstractSparkPublicationsTest {

  test("Select0") {
    val result = new Select0Query(authorsRDD, publicationsRDD).computeResult

    val resultsAsList:Set[List[String]] = result
          .map(a => List(s"name: ${a.name}", s"title: ${a.title}", s"year: ${a.year}"))
          .asInstanceOf[Set[List[String]]]
    val actual = resultsToString(resultsAsList)
    val expected = convertExpected("""
    [name: Neuhauser, B., title: professor, year: 1973]
    [name: Takeno, K., title: PhD, year: 1973]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select1") {
    val result = new Select1Query(authorsRDD, publicationsRDD).computeResult

    val resultsAsList:Set[List[String]] = result
      .map(a => List(s"nom: ${a.nom}", s"titre: ${a.titre}", s"annee: ${a.annee}"))
      .asInstanceOf[Set[List[String]]]
    val actual =  resultsToString(resultsAsList)
    val expected = convertExpected("""
    [annee: 1973, nom: Neuhauser, B., titre: professor]
    [annee: 1973, nom: Takeno, K., titre: PhD]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
