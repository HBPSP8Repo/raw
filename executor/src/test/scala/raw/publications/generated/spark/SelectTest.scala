package raw.publications.generated.spark

import raw._

class SelectTest extends AbstractSparkTest {

  test("Select0") {
    val oql = """
      select distinct a.name, a.title, a.year from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Select0", result)
  }

  test("Select1") {
    val oql = """
      select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Select1", result)
  }

  test("Select2") {
    val oql = """
      select a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Select2", result)
  }

  test("Select3") {
    val oql = """
      select distinct a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Select3", result)
  }

}
