package raw.patients.generated

import raw.patients.AbstractSparkPatientsTest

class SelectTest extends AbstractSparkPatientsTest {

  test("Select0") {
    val oql = """
      count(patients)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
500
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select1") {
    val oql = """
      select P from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
[city: Bern, country: Switzerland, diagnosis: [[code: L51.0, description: Nonbullous erythema multiforme, diag_date: 14/03/2000, diag_id: 2428, patient_id: P00196], [code: L52., description: Erythema nodosum, diag_date: 25/09/2001, diag_id: 2426, patient_id: P00196], [code: L53.8, description: Other specified erythematous conditions, diag_date: 16/10/2006, diag_id: 2427, patient_id: P00196], [code: L55.8, description: Other sunburn, diag_date: 06/10/2011, diag_id: 2425, patient_id: P00196]], gender: female, patient_id: P00196, year_of_birth: 1997]
[city: Guimaraes, country: Portugal, diagnosis: [[code: L53.2, description: Erythema marginatum, diag_date: 17/07/2005, diag_id: 2406, patient_id: P00189], [code: L53.3, description: Other chronic figurate erythema, diag_date: 11/11/2001, diag_id: 2407, patient_id: P00189], [code: L54.0, description: Erythema marginatum in acute rheumatic fever, diag_date: 14/12/2013, diag_id: 2408, patient_id: P00189], [code: L55.0, description: Sunburn of first degree, diag_date: 07/10/2000, diag_id: 2409, patient_id: P00189]], gender: female, patient_id: P00189, year_of_birth: 1996]
[city: Lyon, country: France, diagnosis: [[code: L51.1, description: Bullous erythema multiforme, diag_date: 06/11/2000, diag_id: 2124, patient_id: P00056], [code: L57.1, description: Actinic reticuloid, diag_date: 24/09/2003, diag_id: 2122, patient_id: P00056], [code: L57.3, description: Poikiloderma of Civatte, diag_date: 04/03/2008, diag_id: 2123, patient_id: P00056], [code: L57.9, description: Skin changes due to chronic exposure to nonionizing radiation, unspecified, diag_date: 19/07/2007, diag_id: 2125, patient_id: P00056]], gender: male, patient_id: P00056, year_of_birth: 1997]
[city: Paris, country: France, diagnosis: [[code: L50.8, description: Other urticaria, diag_date: 27/06/2008, diag_id: 2397, patient_id: P00186], [code: L51.1, description: Bullous erythema multiforme, diag_date: 27/01/2012, diag_id: 2396, patient_id: P00186], [code: L57.5, description: Actinic granuloma, diag_date: 12/11/2006, diag_id: 2395, patient_id: P00186], [code: L60.3, description: Nail dystrophy, diag_date: 26/01/2012, diag_id: 2398, patient_id: P00186]], gender: female, patient_id: P00186, year_of_birth: 1995]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select2") {
    val oql = """
      select P.patient_id, P.diagnosis from patients P where count(P.diagnosis) > 3 and year_of_birth > 1994
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
[diagnosis: [[code: L50.8, description: Other urticaria, diag_date: 27/06/2008, diag_id: 2397, patient_id: P00186], [code: L51.1, description: Bullous erythema multiforme, diag_date: 27/01/2012, diag_id: 2396, patient_id: P00186], [code: L57.5, description: Actinic granuloma, diag_date: 12/11/2006, diag_id: 2395, patient_id: P00186], [code: L60.3, description: Nail dystrophy, diag_date: 26/01/2012, diag_id: 2398, patient_id: P00186]], patient_id: P00186]
[diagnosis: [[code: L51.0, description: Nonbullous erythema multiforme, diag_date: 14/03/2000, diag_id: 2428, patient_id: P00196], [code: L52., description: Erythema nodosum, diag_date: 25/09/2001, diag_id: 2426, patient_id: P00196], [code: L53.8, description: Other specified erythematous conditions, diag_date: 16/10/2006, diag_id: 2427, patient_id: P00196], [code: L55.8, description: Other sunburn, diag_date: 06/10/2011, diag_id: 2425, patient_id: P00196]], patient_id: P00196]
[diagnosis: [[code: L51.1, description: Bullous erythema multiforme, diag_date: 06/11/2000, diag_id: 2124, patient_id: P00056], [code: L57.1, description: Actinic reticuloid, diag_date: 24/09/2003, diag_id: 2122, patient_id: P00056], [code: L57.3, description: Poikiloderma of Civatte, diag_date: 04/03/2008, diag_id: 2123, patient_id: P00056], [code: L57.9, description: Skin changes due to chronic exposure to nonionizing radiation, unspecified, diag_date: 19/07/2007, diag_id: 2125, patient_id: P00056]], patient_id: P00056]
[diagnosis: [[code: L53.2, description: Erythema marginatum, diag_date: 17/07/2005, diag_id: 2406, patient_id: P00189], [code: L53.3, description: Other chronic figurate erythema, diag_date: 11/11/2001, diag_id: 2407, patient_id: P00189], [code: L54.0, description: Erythema marginatum in acute rheumatic fever, diag_date: 14/12/2013, diag_id: 2408, patient_id: P00189], [code: L55.0, description: Sunburn of first degree, diag_date: 07/10/2000, diag_id: 2409, patient_id: P00189]], patient_id: P00189]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
