package raw

import java.nio.file.Files

import com.typesafe.scalalogging.StrictLogging

// TODO: File is not being closed. Re
object QueryLogger extends StrictLogging {
  private val queriesLogWriter = {
    val f = Files.createTempFile("raw-queries", ".txt")
    logger.info("Logging generated code to {}", f)
    Files.newBufferedWriter(f)
  }

  def log(query: String, algebra:String, code: String): Unit = {
    val formattedQuery = query.replace("yield", "\nyield")
    val formattedAlgebra = algebra.replace("Spark", "\nSpark")
    queriesLogWriter.write(formattedQuery + "\n" + formattedAlgebra + "\n" + code + "\n")
    queriesLogWriter.newLine()
    queriesLogWriter.flush()
  }

  def close(): Unit = {
    queriesLogWriter.close()
  }
}