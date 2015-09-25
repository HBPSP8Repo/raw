package raw.publications.generated.oql.scala

import org.scalatest.BeforeAndAfterAll
import raw._

class CountTest extends AbstractScalaTest with LDBDockerContainer with BeforeAndAfterAll {

  test("Count0") {
    val queryLanguage = "oql"
    val query = """
      count(authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count0", result)
  }

  test("Count1") {
    val queryLanguage = "oql"
    val query = """
      count(publications)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count1", result)
  }

}
