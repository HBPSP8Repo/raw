package raw.publications.generated.oql.spark

import org.scalatest.BeforeAndAfterAll
import raw._

class SelectTest extends AbstractSparkTest with LDBDockerContainer with BeforeAndAfterAll {

  test("Select0") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select distinct a.name, a.title, a.year from authors a where a.year = 1973
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Select0", result)
  }

  test("Select1") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Select1", result)
  }

  test("Select2") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Select2", result)
  }

  test("Select3") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select distinct a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Select3", result)
  }

}
