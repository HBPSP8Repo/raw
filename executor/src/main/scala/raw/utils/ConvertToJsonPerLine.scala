package raw.utils

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import raw.datasets.patients.Patient
import raw.executionserver.JsonLoader


// TODO: 1) Accept command line arguments, 2) streaming to deal with very large JSON files.
/* SparkSQL requires JSON files to contain one json element per-line. This converts a json file containing an array
 of values into the format expected by SparkSQL.
 */
object ConvertToJsonPerLine {
  private[this] val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]) {
    val data: List[Array[Patient]] = JsonLoader.load[Array[Patient]]("data/patients/patients.json")
    val arr = data.head
    val writer = new StringWriter()
    arr.foreach(p => {
      mapper.writeValue(writer, p)
      writer.write("\n")
    })
    Files.write(Paths.get("/tmp/patients.json"), writer.toString.getBytes(StandardCharsets.UTF_8))
  }
}
