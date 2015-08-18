package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class CollectionExprTest extends AbstractSparkPublicationsTest(Publications.Spark.publications) {

  test("CollectionExpr0") {
    val oql = """
      min(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
1951
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("CollectionExpr1") {
    val oql = """
      max(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
1994
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("CollectionExpr2") {
    val oql = """
      sum(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
98724
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("CollectionExpr4") {
    val oql = """
      count(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
50
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

  test("CollectionExpr6") {
    val oql = """
      EXISTS (select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected("""
true
    """)

    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
