package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class CollectionExprTest extends AbstractSparkPublicationsTest(Publications.Spark.publications) {

  test("CollectionExpr0") {
    val oql = """
      min(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("CollectionExpr0", result)
  }

  test("CollectionExpr1") {
    val oql = """
      max(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("CollectionExpr1", result)
  }

  test("CollectionExpr2") {
    val oql = """
      sum(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("CollectionExpr2", result)
  }

  test("CollectionExpr4") {
    val oql = """
      count(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("CollectionExpr4", result)
  }

  test("CollectionExpr6") {
    val oql = """
      EXISTS (select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("CollectionExpr6", result)
  }

}
