package raw.publications.generated.spark

import raw._
import raw.datasets.publications.Publications

class CountTest extends AbstractSparkTest(Publications.Spark.publications) {

  test("Count0") {
    val oql = """
      count(authors)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("publications", "Count0", result)
  }

  test("Count1") {
    val oql = """
      count(publications)
    """
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual("publications", "Count1", result)
  }

}
