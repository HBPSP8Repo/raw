package raw.publications.generated.qrawl.spark

import raw._

class CollectionExprTest extends AbstractSparkTest {

  test("CollectionExpr0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      min(select a.year from a in authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr0", result)
  }

  test("CollectionExpr1") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      max(select a.year from a in authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr1", result)
  }

  test("CollectionExpr2") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      sum(select a.year from a in authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr2", result)
  }

  test("CollectionExpr4") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      count(select a.year from a in authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr4", result)
  }

  test("CollectionExpr6") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      EXISTS (select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr6", result)
  }

}
