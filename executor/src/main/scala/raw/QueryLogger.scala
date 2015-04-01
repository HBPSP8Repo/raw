package raw

import java.nio.file.Files

// TODO: File is not being closed. Re
object QueryLogger {
  private val queriesLogWriter = {
    val f = Files.createTempFile("raw-queries", ".txt")
    println("Logging generated code to " + f)
    Files.newBufferedWriter(f)
  }

  def log(query: String, code: String): Unit = {
    println("Query: " + query + " -> Code: " + code)
    queriesLogWriter.write(query + "\n" + code + "\n")
    queriesLogWriter.newLine()
    queriesLogWriter.flush
  }

  def close(): Unit = {
    queriesLogWriter.close()
  }
}