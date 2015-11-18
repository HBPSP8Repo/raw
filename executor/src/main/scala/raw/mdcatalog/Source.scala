package raw.mdcatalog

import java.nio.file.{Files, Path}
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.executor.InferrerShellExecutor
import raw.utils.FileTypes


object DataSource extends StrictLogging {

  protected final val jsonMapper = new ObjectMapper()

  def newCsvDataSourceProperties(properties: java.util.Map[String, Object]): CsvDataSourceProperties = {
    val HasHeader = "has_header"
    val Delimiter = "delimiter"
    val FieldNames = "field_names"

    val hasHeader = {
      val v = properties.get(HasHeader)
      if (v == null) None else Some(v.asInstanceOf[Boolean])
    }

    val fieldNames = {
      val v = properties.get(FieldNames)
      if (v == null) None else Some(v.asInstanceOf[util.ArrayList[String]])
    }

    val delimiter = {
      val v = properties.get(Delimiter)
      if (v == null) {
        None
      } else Some({
        val str = v.asInstanceOf[String]
        assert(str.length == 1, s"Expected delimiter to consist of a single char. Was: $str")
        str.charAt(0)
      })
    }

    CsvDataSourceProperties(hasHeader, fieldNames, delimiter)
  }



  def createLocalDataSource(schemaName: String, file: Path): DataSource = {
    logger.info(s"Inferring schema: $file")
    val tmpDir = Files.createTempDirectory("schema-inference")
    try {
      val fileType = FileTypes.inferFileType(file)
      InferrerShellExecutor.inferSchema(file, fileType, tmpDir)

      logger.info(s"Loading schema: $schemaName at directory: $tmpDir")
      val properties = tmpDir.resolve("properties.json")
      val userData = jsonMapper.readValue(properties.toFile, classOf[java.util.Map[String, Object]])

      var attributeOrder: Option[util.ArrayList[String]] = None
      val location: Location = LocalFile(file)
      val format = fileType match {
        case FileTypes.Json => JSON()
        case FileTypes.Csv => {
          val schemaProperties = DataSource.newCsvDataSourceProperties(userData)
          attributeOrder = schemaProperties.fieldNames
          CSV(schemaProperties)
        }
        case FileTypes.Text => Text()
      }
      val schemaFile = tmpDir.resolve("schema.xml")
      val tipe = SchemaParser(schemaFile, attributeOrder)
      logger.info(s"Raw Type: $tipe")
      val length = file.toFile.length()
      val size: Option[Long] = if (length == 0) None else Some(length)
      val accessPaths: Set[AccessPath] = Set(SequentialAccessPath(location, format, size))
      DataSource(schemaName, tipe, accessPaths)
    } finally {
      FileUtils.deleteQuietly(tmpDir.toFile)
    }
  }
}

sealed abstract class Source(name: String) {

}

case class DataSource(name: String, tipe: raw.Type, accessPaths: Set[AccessPath]) extends Source(name) {

}

case class ViewSource(name: String, view: View) extends Source(name) {

}
