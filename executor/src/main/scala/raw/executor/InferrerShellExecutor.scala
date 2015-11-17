package raw.executor

import java.io.{ByteArrayOutputStream, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import raw.rest.ClientErrorException

import scala.sys.process._

object InferrerShellExecutor extends StrictLogging {
  private[this] val inferrerPath = {
    val value = ConfigFactory.load().getString("raw.inferrer.path")
    val p = Paths.get(value).resolve("inferrer.py")
    logger.info(s"Path: $value => $p")
    if (!Files.exists(p)) {
      logger.warn(s"Inferrer not found: $p. Proceeding but registering files will not be possible.")
    }
    p
  }

  /* Runs a command in the shell, returning the status code and the output (stdout and stderro) */
  def runCommand(cmd: String): (Int, String) = {
    val output = new ByteArrayOutputStream
    //    val stderr = new ByteArrayOutputStream
    val outputWriter = new PrintWriter(output)
    //    val stderrWriter = new PrintWriter(stderr)
    val exitValue = cmd.!(ProcessLogger(outputWriter.println, outputWriter.println))
    outputWriter.close()
    //    stderrWriter.close()
    (exitValue, output.toString)
  }

  def inferSchema(filePath: Path, fileType: String, schemaName: String): Unit = {
    inferSchema(filePath, fileType, schemaName, filePath.getParent)
  }

  def inferSchema(filePath: Path, fileType: String, schemaName: String, outputPath: Path): Unit = {
    val cmdLine = s"python ${inferrerPath.toString} -f ${filePath.toString} -t $fileType -n $schemaName -o ${outputPath.toString}"
    logger.info(s"Executing command: $cmdLine")
    val start = Stopwatch.createStarted()
    val (s, output) = runCommand(cmdLine)
    // TODO: Distinguish between inferrer failures because of bad input (client error) versus other problems/bugs (internal error)
    // Currently, assume that return status 1 means bad input and anything else (other than 0) means bug.
    if (s == 1) {
      throw new ClientErrorException(s"Failed to infer schema. Inferrer output:\n$output")
    }
    assert(s == 0, s"Error executing command (status:$s): $cmdLine. Output:\n$output")
    logger.info("Inferred schema in " + start.elapsed(TimeUnit.MILLISECONDS) + "ms")
  }
}