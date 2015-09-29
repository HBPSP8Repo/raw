package raw.publications.generated.qrawl.scala

import raw._

class GroupByTest extends AbstractScalaTest {

  test("GroupBy0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.title AS title, count(partition) as n from authors A group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy0", result)
  }

  test("GroupBy1") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.title AS title, (select distinct a.year from a in partition) as years from authors A group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy1", result)
  }

  test("GroupBy2") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select distinct A.year as year, (select distinct A from partition A) as people from authors A group by A.year
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy2", result)
  }

  test("GroupBy3") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select A.title as title,
       (select P from partition P where P = A) as people
from authors A
group by A.title
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "GroupBy3", result)
  }

}
