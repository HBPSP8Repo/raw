package raw.executor

import java.net.URI
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util
import java.util.Collections
import java.util.function.BiPredicate

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.utils.RawUtils

import scala.collection.{JavaConversions, mutable}

case class RawSchema(name: String, schemaFile: Path, properties: SchemaProperties, dataFile: Path)

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

  override def toString(): String = properties.toString
}


object StorageManager extends StrictLogging {
  final val defaultDataDir = RawUtils.getTemporaryDirectory().resolve("rawschemas")
  var storagePath: Path = _

  private[this] final val jsonMapper = new ObjectMapper()
  // Map from (user, schema) -> schemaScanner
  private[this] val scanners = new mutable.HashMap[(String, String), RawScanner[_]]

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
    // Create directory if it does not exist
    RawUtils.createDirectory(p)
    storagePath = p
    loadScannersFromDisk()
  }

  private[this] def loadScannersFromDisk(): Unit = {
    scanners.clear()
    val users = listUsers()
    logger.info("Found users: " + users)
    users.foreach(user => {
      val schemas = listUserSchemasFromDisk(user)
      schemas.foreach(schemaName => {
        val schema = loadSchema(user, schemaName)
        val scanner = CodeGenerator.loadScanner(schemaName, schema)
        logger.info("Created scanner: " + scanner)
        scanners.put((user, schemaName), scanner)
      })
    }
    )
  }

  private[this] def listUserSchemasFromDisk(rawUser: String): Set[String] = {
    val userDataDir = getUserStorageDir(rawUser)
    val directories = JavaConversions.asScalaIterator(Files.list(userDataDir).iterator())
    directories.map(dir => dir.getFileName.toString).to[Set]
  }


  def listUsers(): Set[String] = {
    val directories = JavaConversions.asScalaIterator(Files.list(storagePath).iterator())
    directories.map(dir => dir.getFileName.toString).toSet
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


  def registerSchema2(schemaName: String, dataDirectory: String, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, directory: $dataDirectory, user: $rawUser")
    val uri = new URI(dataDirectory)

    val userDataDir = getUserStorageDir(rawUser)

    val finalDir = userDataDir.resolve(schemaName)
    logger.info(s"Moving to final destination: $finalDir")
    FileUtils.deleteDirectory(finalDir.toFile)

    val tmpDir = storagePath.resolve(dataDirectory)
    Files.move(tmpDir, finalDir)

    val schema = loadSchema(rawUser, schemaName)
    val scanner = CodeGenerator.loadScanner(schemaName, schema)
    logger.info("Created scanner: " + scanner)
    scanners.put((rawUser, schemaName), scanner)
  }

  def loadSchema(rawUser: String, schemaName: String): RawSchema = {
    val schemaDir = getUserStorageDir(rawUser).resolve(schemaName)
    logger.info(s"Loading schema: $schemaName")

    val bp = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        if (u.isDirectory) return false
        val s = t.getFileName.toString
        s.startsWith(schemaName + ".")
      }
    }
    val iter = JavaConversions.asScalaIterator(Files.find(schemaDir, 1, bp).iterator())
    val list = iter.toList
    assert(list.size == 1, s"Expected one data file for schema: $schemaName in directory: $schemaDir. Found: $list.")

    val properties = schemaDir.resolve("properties.json")
    val userData = jsonMapper.readValue(properties.toFile, classOf[java.util.Map[String, Object]])

    RawSchema(schemaName, schemaDir.resolve("schema.xml"), new SchemaProperties(userData), list.head)
  }

  def listUserSchemas(rawUser: String): Set[String] = {
    scanners.keys.filter(p => p._1 == rawUser).map(p => p._2).toSet
  }

  def getScanner(rawUser: String, schemaName: String): RawScanner[_] = {
    logger.info(s"Getting scanner for $rawUser, $schemaName")
    scanners((rawUser, schemaName))
  }
}
