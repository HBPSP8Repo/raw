package raw.publications.generated.qrawl.spark

import raw._

class GroupByTest extends AbstractSparkTest {

  test("GroupBy0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.title, count(partition) as n from authors A group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy0", result)
  }

  test("GroupBy1") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.title, (select distinct a.year from a in partition) as years from authors A group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy1", result)
  }

  test("GroupBy2") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.year, (select distinct A from partition A) as people from authors A group by A.year
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy2", result)
  }

  test("GroupBy3") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select A.title,
       partition as people
from authors A
group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy3", result)
  }

}
