package raw.publications.generated.qrawl.scala

import raw._

class CountTest extends AbstractScalaTest {

  test("Count0") {
    val queryLanguage = "qrawl"
    val query = """
      count(authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count0", result)
  }

  test("Count1") {
    val queryLanguage = "qrawl"
    val query = """
      count(publications)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Count1", result)
  }

}
