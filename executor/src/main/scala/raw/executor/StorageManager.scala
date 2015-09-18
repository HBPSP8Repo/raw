package raw.executor

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util
import java.util.Collections
import java.util.function.BiPredicate

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.executor.CsvLoader._
import raw.utils.RawUtils

import scala.collection.JavaConversions

case class RawSchema(schemaFile: Path, properties: SchemaProperties, dataFile: Path)

class SchemaProperties(schemaProperties: java.util.Map[String, Object]) {
  final val HAS_HEADER = "has_header"
  final val FIELD_NAMES = "field_names"

  // For convenience, convert null argument to an empty map
  val properties = if (schemaProperties == null) Collections.emptyMap[String, Object]() else schemaProperties

  def hasHeader(): Option[Boolean] = {
    val v = properties.get(HAS_HEADER)
    if (v == null) None else Some(v.asInstanceOf[Boolean])
  }

  def fieldNames(): Option[util.ArrayList[String]] = {
    val v = properties.get(FIELD_NAMES)
    if (v == null) None else Some(v.asInstanceOf[util.ArrayList[String]])
  }
}


object StorageManager extends StrictLogging {
  final val defaultDataDir = RawUtils.getTemporaryDirectory().resolve("rawschemas")

  var storagePath: Path = _

  {
    val rawDir = try {
      Paths.get(ConfigFactory.load().getString("raw.datadir"))
    } catch {
      case ex: ConfigException.Missing => defaultDataDir
    }
    setStoragePath(rawDir)
  }

  def setStoragePath(p: Path) = {
    logger.info("Storing data files at: " + p)
    RawUtils.createDirectory(p)
    storagePath = p
  }

  def extractFilename(uri: URI): String = {
    val s = uri.getPath
    val startIndex = s.lastIndexOf("/") + 1
    s.substring(startIndex)
  }

  private[this] def getUserStorageDir(user: String): Path = {
    val userDataDir = storagePath.resolve(user)
    if (Files.notExists(userDataDir)) {
      RawUtils.createDirectory(userDataDir)
    }
    userDataDir
  }

  def registerSchema(schemaName: String, dataDirectory: String, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, directory: $dataDirectory, user: $rawUser")
    val uri = new URI(dataDirectory)

    val userDataDir = getUserStorageDir(rawUser)

    val finalDir = userDataDir.resolve(schemaName)
    logger.info(s"Moving to final destination: $finalDir")
    FileUtils.deleteDirectory(finalDir.toFile)

    val tmpDir = storagePath.resolve(dataDirectory)
    Files.move(tmpDir, finalDir)

    //    /* Use a temporary directory to store the schema and the data file. This way, when overwriting a schema, we don't
    //     * delete the previous version of a schema until the new one is safely written on disk
    //     */
    //    val tmpDir = Files.createTempDirectory("rawdata")
    //
    //    val schemaPath = tmpDir.resolve(schemaName + schemaExtension)
    //    logger.info(s"Writing schema: $schemaPath")
    //    Files.write(schemaPath, xmlSchema.getBytes(StandardCharsets.UTF_8))
    //
    //    val dataFilePath = tmpDir.resolve(extractFilename(uri))
    //    logger.info(s"Writing data: $dataFilePath")
    //    val localFile = if (uri.getScheme().startsWith("http")) {
    //      logger.info("toURL: " + uri.toURL)
    //      val is = uri.toURL.openStream()
    //      Files.copy(is, dataFilePath, StandardCopyOption.REPLACE_EXISTING)
    //      is.close()
    //      dataFilePath
    //    } else {
    //      val src = Paths.get(uri)
    //      Files.copy(src, dataFilePath, StandardCopyOption.REPLACE_EXISTING)
    //    }
    //
    //    val finalDir = userDataDir.resolve(schemaName)
    //    logger.info(s"Moving to final destination: $finalDir")
    //    FileUtils.deleteDirectory(finalDir.toFile)
    //    Files.move(tmpDir, finalDir)
  }

  def listSchemas(rawUser: String): Seq[String] = {
    val userDataDir = getUserStorageDir(rawUser)
    val directories = JavaConversions.asScalaIterator(Files.list(userDataDir).iterator())
    directories.map(dir => dir.getFileName.toString).to[List]
  }

  private[this] final val jsonMapper = new ObjectMapper()

  def getSchema(rawUser: String, schema: String): RawSchema = {
    val schemaDir = getUserStorageDir(rawUser).resolve(schema)
    logger.info(s"Loading schema: $schema")

    val bp = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        if (u.isDirectory) return false
        val s = t.getFileName.toString
        s.startsWith(schema + ".")
      }
    }
    val iter = JavaConversions.asScalaIterator(Files.find(schemaDir, 1, bp).iterator())
    val list = iter.toList
    assert(list.size == 1, s"Expected one data file for schema: $schema in directory: $schemaDir. Found: $list.")

    val properties = schemaDir.resolve("properties.json")
    val userData = jsonMapper.readValue(properties.toFile, classOf[java.util.Map[String, Object]])
    logger.info("Userdata: " + userData)

    RawSchema(schemaDir.resolve("schema.xml"), new SchemaProperties(userData), list.head)
  }
}
