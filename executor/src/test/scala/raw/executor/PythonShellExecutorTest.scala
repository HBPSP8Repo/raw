package raw.executor

import java.nio.file.Paths

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSuite

class PythonShellExecutorTest extends FunSuite with StrictLogging {

  test("Hello") {
    val fileType = "json"
    val filePath = Paths.get("/tmp/raw-stage6848337328611766446/array2d.json")
    val schemaName = "array2d"
    PythonShellExecutor.inferSchema(filePath, fileType, schemaName)
  }
}
