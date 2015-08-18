package raw.patients.generated

import raw.patients.AbstractSparkPatientsTest

class TrialsTest extends AbstractSparkPatientsTest {

  test("Trials0") {
    val oql = """
      select G, count(partition) from patients P group by G: struct(a:P.city, b:P.country)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
[G: [a: Bern, b: Switzerland], _X0: 33]
[G: [a: Bordeaux, b: France], _X0: 36]
[G: [a: Florence, b: Italy], _X0: 35]
[G: [a: Geneva, b: Switzerland], _X0: 29]
[G: [a: Guimaraes, b: Portugal], _X0: 45]
[G: [a: Lisbon, b: Portugal], _X0: 43]
[G: [a: Lyon, b: France], _X0: 26]
[G: [a: Marseille, b: France], _X0: 37]
[G: [a: Milan, b: Italy], _X0: 40]
[G: [a: Oporto, b: Portugal], _X0: 41]
[G: [a: Paris, b: France], _X0: 29]
[G: [a: Rome, b: Italy], _X0: 40]
[G: [a: Zurich, b: Switzerland], _X0: 29]
[G: [a: lausanne, b: Switzerland], _X0: 37]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Trials1") {
    val oql = """
      select distinct I.gender, (select distinct country, count(partition) from I.people P group by country:P.country) as G from (
select distinct gender, (select P from partition) as people from patients P group by gender: P.gender
) I
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
[G: [[_X0: 56, country: Italy], [_X0: 58, country: Portugal], [_X0: 64, country: France], [_X0: 70, country: Switzerland]], gender: male]
[G: [[_X0: 58, country: Switzerland], [_X0: 59, country: Italy], [_X0: 64, country: France], [_X0: 71, country: Portugal]], gender: female]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Trials2") {
    val oql = """
      select distinct T.gender, (select city, (select distinct patient_id from partition) as c from T.people P group by city: P.city) as X from
(select distinct gender, (select p from partition) as people from patients p group by gender: p.gender) T
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
[X: [[c: [P00000, P00002, P00011, P00014, P00016, P00059, P00088, P00148, P00182, P00216, P00229, P00247, P00258, P00270, P00339, P00355, P00358, P00384, P00405, P00438, P00452], city: Geneva], [c: [P00003, P00029, P00055, P00123, P00126, P00179, P00203, P00237, P00254, P00288, P00316, P00323, P00359, P00418, P00446], city: Zurich], [c: [P00004, P00010, P00015, P00038, P00057, P00067, P00134, P00151, P00173, P00193, P00211, P00301, P00314, P00320, P00334, P00354, P00400, P00420, P00431, P00456, P00462, P00479], city: Marseille], [c: [P00005, P00053, P00056, P00125, P00185, P00197, P00215, P00263, P00281, P00296, P00319, P00333, P00393, P00412], city: Lyon], [c: [P00007, P00013, P00020, P00062, P00065, P00066, P00130, P00172, P00277, P00280, P00298, P00346, P00388, P00392, P00426, P00444, P00453, P00455, P00457, P00459, P00480, P00488], city: Milan], [c: [P00008, P00047, P00061, P00063, P00129, P00137, P00153, P00163, P00195, P00209, P00225, P00313, P00419, P00429, P00463, P00495], city: Rome], [c: [P00017, P00023, P00037, P00054, P00082, P00113, P00135, P00155, P00190, P00191, P00265, P00272, P00274, P00338, P00360, P00371, P00381, P00432], city: Bern], [c: [P00022, P00035, P00118, P00162, P00226, P00231, P00289, P00317, P00357, P00395, P00449, P00451, P00473, P00484, P00497], city: Bordeaux], [c: [P00028, P00058, P00064, P00069, P00079, P00106, P00111, P00120, P00146, P00159, P00176, P00261, P00293, P00299, P00324, P00327, P00352, P00365, P00372, P00402, P00417, P00482], city: Guimaraes], [c: [P00030, P00050, P00076, P00084, P00117, P00175, P00218, P00221, P00279, P00290, P00311, P00379, P00409, P00416, P00428, P00437, P00475, P00499], city: Florence], [c: [P00040, P00070, P00093, P00105, P00124, P00147, P00170, P00192, P00214, P00222, P00232, P00273, P00292, P00356, P00470, P00476, P00496], city: Lisbon], [c: [P00045, P00095, P00098, P00142, P00188, P00212, P00302, P00328, P00362, P00364, P00370, P00378, P00403, P00410, P00422, P00434, P00440, P00469, P00483], city: Oporto], [c: [P00048, P00052, P00085, P00108, P00131, P00132, P00161, P00194, P00227, P00249, P00266, P00276, P00303, P00332, P00369, P00413], city: lausanne], [c: [P00086, P00128, P00178, P00230, P00264, P00267, P00285, P00318, P00331, P00342, P00407, P00439, P00450], city: Paris]], gender: male]
[X: [[c: [P00001, P00042, P00089, P00116, P00145, P00152, P00181, P00183, P00349, P00361, P00396, P00443], city: Lyon], [c: [P00006, P00033, P00043, P00060, P00087, P00115, P00136, P00138, P00213, P00262, P00275, P00348, P00380, P00386, P00414, P00460, P00471], city: Florence], [c: [P00009, P00026, P00027, P00034, P00041, P00072, P00080, P00083, P00174, P00235, P00252, P00253, P00255, P00278, P00286, P00300, P00304, P00315, P00347, P00350, P00351, P00376, P00411, P00423, P00464, P00492], city: Lisbon], [c: [P00012, P00021, P00036, P00103, P00107, P00144, P00199, P00207, P00210, P00244, P00251, P00283, P00330, P00367, P00458, P00461, P00478, P00491], city: Milan], [c: [P00018, P00049, P00097, P00109, P00110, P00119, P00139, P00189, P00201, P00219, P00223, P00241, P00242, P00256, P00269, P00284, P00335, P00387, P00448, P00468, P00474, P00489, P00494], city: Guimaraes], [c: [P00019, P00164, P00196, P00198, P00238, P00250, P00308, P00310, P00336, P00337, P00343, P00375, P00385, P00399, P00424], city: Bern], [c: [P00024, P00044, P00068, P00092, P00122, P00149, P00167, P00200, P00208, P00239, P00240, P00329, P00341, P00353, P00389, P00397, P00425, P00430, P00441, P00487, P00493], city: lausanne], [c: [P00025, P00031, P00046, P00081, P00099, P00101, P00112, P00154, P00157, P00187, P00205, P00224, P00228, P00234, P00248, P00282, P00287, P00321, P00383, P00390, P00415, P00467, P00485, P00490], city: Rome], [c: [P00032, P00102, P00133, P00143, P00206, P00246, P00306, P00481], city: Geneva], [c: [P00039, P00090, P00140, P00168, P00171, P00177, P00245, P00259, P00291, P00294, P00295, P00309, P00312, P00345, P00373, P00408, P00442, P00445, P00454, P00472, P00486, P00498], city: Oporto], [c: [P00051, P00078, P00091, P00114, P00150, P00169, P00233, P00243, P00257, P00297, P00344, P00398, P00401, P00406, P00447], city: Marseille], [c: [P00071, P00077, P00121, P00166, P00271, P00322, P00340, P00366, P00374, P00391, P00404, P00427, P00433, P00465], city: Zurich], [c: [P00073, P00074, P00075, P00094, P00127, P00158, P00160, P00180, P00184, P00202, P00204, P00217, P00236, P00260, P00325, P00363, P00368, P00377, P00382, P00421, P00436], city: Bordeaux], [c: [P00096, P00100, P00104, P00141, P00156, P00165, P00186, P00220, P00268, P00305, P00307, P00326, P00394, P00435, P00466, P00477], city: Paris]], gender: female]
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
