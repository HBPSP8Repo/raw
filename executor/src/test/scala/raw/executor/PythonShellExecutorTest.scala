package raw.executor

import java.nio.file.Paths

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite
import raw.TestScanners
import raw.utils.RawUtils

class PythonShellExecutorTest extends FunSuite with StrictLogging with InferrerConfiguration {

  test("Can call inferrer") {
    val fileType = "json"
    val schemaName = "array2d"
    PythonShellExecutor.inferSchema(TestScanners.authorsPath, fileType, schemaName)
  }
}
