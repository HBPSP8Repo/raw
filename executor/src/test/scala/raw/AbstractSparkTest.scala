package raw

import org.apache.spark.SparkContext
import raw.datasets.AccessPath

abstract class AbstractSparkTest(loader: (SparkContext) => List[AccessPath[_ <: Product]])
  extends AbstractRawTest
  with SharedSparkContext {

  var accessPaths: List[AccessPath[_ <: Product]] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      accessPaths = loader(sc)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
