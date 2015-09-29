package raw.publications.generated.qrawl.spark

import raw._

class UnnestTest extends AbstractSparkTest {

  test("Unnest0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select A as author from publications P, P.authors A
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publicationsSmall", "Unnest0", result)
  }

}
