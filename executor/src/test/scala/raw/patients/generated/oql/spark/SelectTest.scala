package raw.patients.generated.oql.spark

import org.scalatest.BeforeAndAfterAll
import raw._

class SelectTest extends AbstractSparkTest with LDBDockerContainer with BeforeAndAfterAll {

  test("Select0") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      count(patients)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select0", result)
  }

  test("Select1") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select P from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select1", result)
  }

  test("Select2") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      select P.patient_id, P.diagnosis from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("patients", "Select2", result)
  }

}
