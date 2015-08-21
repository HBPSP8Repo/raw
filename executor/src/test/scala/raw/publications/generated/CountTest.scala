package raw.publications.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class CountTest extends AbstractSparkPublicationsTest(Publications.Spark.publications) {

  test("Count0") {
    val oql = """
      count(authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Count0", result)
  }

  test("Count1") {
    val oql = """
      count(publications)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("Count1", result)
  }

}
