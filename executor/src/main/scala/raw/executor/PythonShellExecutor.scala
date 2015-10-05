package raw.executor

import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.sys.process._

object PythonShellExecutor extends StrictLogging {
  private[this] val inferrerPath = {
    val value = ConfigFactory.load().getString("raw.inferrer.path")
    val p = Paths.get(value).resolve("inferrer.py")
    logger.info(s"Path: $value => $p")
    if (!Files.exists(p)) {
      logger.warn(s"Inferrer not found: $p. Proceeding but registering files will not be possible.")
    }
    p
  }

  def inferSchema(filePath: Path, fileType: String, schemaName: String): Unit = {
    val cmdLine = s"python ${inferrerPath.toString} -f ${filePath.toString} -t $fileType -n $schemaName"
    logger.info(s"Executing command: $cmdLine")
    val s: Int = cmdLine.!
    assert(s == 0, s"Error executing command (status:$s): $cmdLine")
  }
}