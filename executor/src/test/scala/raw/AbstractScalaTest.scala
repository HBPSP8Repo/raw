package raw

import raw.datasets.AccessPath

abstract class AbstractScalaTest(loader: () => List[AccessPath[_ <: Product]])
  extends AbstractRawTest {

  var accessPaths: List[AccessPath[_ <: Product]] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      accessPaths = loader()
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
