package raw.publications.generated.spark

import raw._
import raw.datasets.publications.Publications

class AssignTest extends AbstractSparkTest(Publications.Spark.publications) {

  test("Assign0") {
    val oql = """
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
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("publications", "Assign0", result)
  }

}
