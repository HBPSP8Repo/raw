package raw.publications.generated.scala

import raw._

class CollectionExprTest extends AbstractScalaTest {

  test("CollectionExpr0") {
    val oql = """
      min(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr0", result)
  }

  test("CollectionExpr1") {
    val oql = """
      max(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr1", result)
  }

  test("CollectionExpr2") {
    val oql = """
      sum(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr2", result)
  }

  test("CollectionExpr4") {
    val oql = """
      count(select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr4", result)
  }

  test("CollectionExpr6") {
    val oql = """
      EXISTS (select year from authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr6", result)
  }

}
