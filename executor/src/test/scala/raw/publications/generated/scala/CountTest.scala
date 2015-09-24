package raw.publications.generated.scala

import raw._

class CountTest extends AbstractScalaTest {

  test("Count0") {
    val oql = """
      count(authors)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Count0", result)
  }

  test("Count1") {
    val oql = """
      count(publications)
    """
    val result = queryCompiler.compileOQL(oql, scanners).computeResult
    assertJsonEqual("publications", "Count1", result)
  }

}
