package raw

import java.nio.file.{Files, Path, Paths}

import com.typesafe.scalalogging.StrictLogging

object QueryLogger extends StrictLogging {
  private[this] var outputDirectory: Path = {
    val tmp = Paths.get(System.getProperty("java.io.tmpdir"))
    tmp
  }
  private[this] var i = -1

  def setOutputDirectory(path: Path): Unit = {
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