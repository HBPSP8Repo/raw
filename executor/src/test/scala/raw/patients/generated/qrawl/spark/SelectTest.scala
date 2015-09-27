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

  test("Select1") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select1", result)
  }

  test("Select2") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P.patient_id, P.diagnosis from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select2", result)
  }

}
