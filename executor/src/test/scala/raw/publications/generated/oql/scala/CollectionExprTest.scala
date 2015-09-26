package raw.publications.generated.oql.scala

import org.scalatest.BeforeAndAfterAll
import raw._

class CollectionExprTest extends AbstractScalaTest with LDBDockerContainer with BeforeAndAfterAll {

  test("CollectionExpr0") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      min(select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr0", result)
  }

  test("CollectionExpr1") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      max(select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr1", result)
  }

  test("CollectionExpr2") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      sum(select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr2", result)
  }

  test("CollectionExpr4") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      count(select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr4", result)
  }

  test("CollectionExpr6") {
    val queryLanguage = QueryLanguages("oql")
    val query = """
      EXISTS (select year from authors)
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "CollectionExpr6", result)
  }

}
