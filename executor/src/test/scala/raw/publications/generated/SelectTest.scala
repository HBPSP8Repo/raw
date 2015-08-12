package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class SelectTest extends AbstractSparkPublicationsTest(Publications.publications) {

  test("Select0") {
    val oql = """
          select distinct a.name, a.title, a.year from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)
    val expected = convertExpected("""
    [name: Neuhauser, B., title: professor, year: 1973]
    [name: Takeno, K., title: PhD, year: 1973]
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select1") {
    val oql = """
          select distinct a.name as nom, a.title as titre, a.year as annee from authors a where a.year = 1973
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)
    val expected = convertExpected("""
    [annee: 1973, nom: Neuhauser, B., titre: professor]
    [annee: 1973, nom: Takeno, K., titre: PhD]
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select2") {
    val oql = """
          select a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)
    val expected = convertExpected("""
    PhD
    assistant professor
    assistant professor
    professor
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Select3") {
    val oql = """
          select distinct a.title from authors a where a.year = 1959
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)
    val expected = convertExpected("""
    PhD
    assistant professor
    professor
    """)
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
