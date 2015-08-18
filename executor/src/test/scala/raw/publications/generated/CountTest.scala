package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class CountTest extends AbstractSparkPublicationsTest(Publications.publications) {

  test("Count0") {
    val oql = """
      count(authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
50
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("Count1") {
    val oql = """
      count(publications)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
1000
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
