package raw.publications.generated.oql.spark

import org.scalatest.BeforeAndAfterAll
import raw._

class CountTest extends AbstractSparkTest with LDBDockerContainer with BeforeAndAfterAll {

  test("Count0") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      count(authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count0", result)
  }

  test("Count1") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      count(publications)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count1", result)
  }

}
