package raw.publications.generated.oql.spark

import org.scalatest.BeforeAndAfterAll
import raw._

class GroupByTest extends AbstractSparkTest with LDBDockerContainer with BeforeAndAfterAll {

  test("GroupBy0") {
    val queryLanguage = "oql"
    val query = """
      select distinct title, count(partition) as n from authors A group by title: A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy0", result)
  }

  test("GroupBy1") {
    val queryLanguage = "oql"
    val query = """
      select distinct title, (select distinct year from partition) as years from authors A group by title: A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy1", result)
  }

  test("GroupBy2") {
    val queryLanguage = "oql"
    val query = """
      select distinct year, (select distinct A from partition) as people from authors A group by title: A.year
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy2", result)
  }

  test("GroupBy3") {
    val queryLanguage = "oql"
    val query = """
      select title,
       (select A from partition) as people
from authors A
group by title: A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy3", result)
  }

}
