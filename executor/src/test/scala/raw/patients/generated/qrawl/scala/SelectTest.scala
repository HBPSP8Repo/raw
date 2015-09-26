package raw.patients.generated.qrawl.scala

import raw._

class SelectTest extends AbstractScalaTest {

  test("Select0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      count(patients)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select0", result)
  }

}
