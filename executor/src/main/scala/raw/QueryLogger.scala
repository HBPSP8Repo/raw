package raw

import java.nio.file.{Files, Path, Paths}

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils

// TODO: File is not being closed.
object QueryLogger extends StrictLogging {
  private[this] var outputDirectory: Path = {
    val tmp = Paths.get(System.getProperty("java.io.tmpdir"), "raw-macro-generated")
    prepareDirectory(tmp)
    tmp
  }
  private[this] var i = -1

  private[this] def prepareDirectory(path: Path) = {
    if (Files.exists(path)) {
      FileUtils.cleanDirectory(path.toFile)
    } else {
      Files.createDirectories(path)
    }
  }

  def setOutputDirectory(path: Path): Unit = {
    prepareDirectory(path)
    this.outputDirectory = path
  }

  def log(query: String, algebra: String, code: String): Unit = {
    i += 1
    val f = outputDirectory.resolve(s"query$i-macro.scala")
    logger.info("Logging generated code to {}", f)
    val writer = Files.newBufferedWriter(f)
    val formattedQuery = query.replace("yield", "\nyield")
    val formattedAlgebra = algebra.replace("Spark", "\nSpark")
    writer.write(formattedQuery + "\n" + formattedAlgebra + "\n" + code + "\n")
    writer.newLine()
    writer.close()
  }
}