package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import raw.TestDatasources

class InferrerShellExecutorTest extends FunSuite with StrictLogging with InferrerConfiguration {

  test("Can call inferrer") {
    val fileType = "json"
    val schemaName = "array2d"
    InferrerShellExecutor.inferSchema(TestDatasources.authorsPath, fileType)
  }
}
