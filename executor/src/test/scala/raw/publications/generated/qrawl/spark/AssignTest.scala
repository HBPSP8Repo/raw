package raw.publications.generated.qrawl.spark

import raw._

class AssignTest extends AbstractSparkTest {

  test("Assign0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select G.title,
      (select year: v,
              N: count(partition)
       from v in G.values
       group by year: v) as stats
from (
      select distinct title, (select year from partition) as values
      from authors A
      group by title: A.title) G
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Assign0", result)
  }

}
