package raw.publications.generated.qrawl.scala

import raw._

class DemoTest extends AbstractScalaTest {

  test("Demo0") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select A from authors A
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo0", result)
  }

  test("Demo1") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo1", result)
  }

  test("Demo2") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select author as author,
       (select P from partition P) as articles
from publications P,
     author in P.authors
where "particle detectors" in P.controlledterms
group by author: author
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo2", result)
  }

  test("Demo3") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo3", result)
  }

  test("Demo4") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo4", result)
  }

  test("Demo5") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo5", result)
  }

  test("Demo6") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo6", result)
  }

  test("Demo7") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo7", result)
  }

  test("Demo8") {
    val queryLanguage = QueryLanguages("qrawl")
    val query = """
      select P from publications P
    """
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual("publications", "Demo8", result)
  }

}
