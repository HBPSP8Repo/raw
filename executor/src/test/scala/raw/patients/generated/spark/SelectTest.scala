package raw.patients.generated.spark

import raw._

class SelectTest extends AbstractSparkTest {

  test("Select0") {
    val oql = """
      count(patients)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("patients", "Select0", result)
  }

  test("Select1") {
    val oql = """
      select P from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("patients", "Select1", result)
  }

  test("Select2") {
    val oql = """
      select P.patient_id, P.diagnosis from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("patients", "Select2", result)
  }

}
