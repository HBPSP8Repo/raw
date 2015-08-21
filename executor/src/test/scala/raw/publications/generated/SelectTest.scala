package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class SelectTest extends AbstractSparkPublicationsTest(Publications.Spark.publications) {

  test("Select0") {
    val oql = """
      select distinct a.name, a.title, a.year from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Select0", result)
  }

  test("Select1") {
    val oql = """
      select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Select1", result)
  }

  test("Select2") {
    val oql = """
      select a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Select2", result)
  }

  test("Select3") {
    val oql = """
      select distinct a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Select3", result)
  }

}
