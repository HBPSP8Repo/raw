package raw.publications

import raw.AbstractSparkTest
import raw.datasets.publications.Publications


class SparkPublicationsDupsTest extends AbstractSparkTest(Publications.Spark.publicationsSmallDups) {

  test("duplicates") {
    val oql = """
    select author, (select P from partition) as articles
    from publications P, P.authors A
    group by author: A"""

    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)
    println("Result:\n" + actual)
  }
}
