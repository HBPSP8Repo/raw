package raw.publications.generated.spark

import raw._

class GroupByTest extends AbstractSparkTest {

  test("GroupBy0") {
    val oql = """
      select distinct title, count(partition) as n from authors A group by title: A.title
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "GroupBy0", result)
  }

  test("GroupBy1") {
    val oql = """
      select distinct title, (select distinct year from partition) as years from authors A group by title: A.title
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "GroupBy1", result)
  }

  test("GroupBy2") {
    val oql = """
      select distinct year, (select distinct A from partition) as people from authors A group by title: A.year
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "GroupBy2", result)
  }

  test("GroupBy3") {
    val oql = """
      select title,
       (select A from partition) as people
from authors A
group by title: A.title
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "GroupBy3", result)
  }

}
