package raw.patients.generated.qrawl.spark

import raw._

class SelectTest extends AbstractSparkTest {

  test("Select0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      count(patients)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select0", result)
  }

}
