package raw

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.datasets.AccessPath
import raw.executionserver._
import raw.perf.QueryCompilerClient

abstract class AbstractSparkTest(loader: (SparkContext) => List[AccessPath[_ <: Product]])
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter
  with LDBDockerContainer {

  var queryCompiler: QueryCompilerClient = _
  var accessPaths: List[AccessPath[_ <: Product]] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      queryCompiler = new QueryCompilerClient(rawClassLoader)
      accessPaths = loader(sc)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
