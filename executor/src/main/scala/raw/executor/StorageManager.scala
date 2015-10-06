package raw.executor

import java.net.URI
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.function.BiPredicate

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.utils.RawUtils

import scala.collection.{JavaConversions, mutable}


object StorageManager {
  val defaultStorageDir = RawUtils.getTemporaryDirectory().resolve("rawschemas")
}

class StorageManager(val storagePath: Path = StorageManager.defaultStorageDir) extends StrictLogging {
  private[this] final val jsonMapper = new ObjectMapper()
  // Map from (user, schema) -> schemaScanner
  private[this] val scanners = new mutable.HashMap[(String, String), RawScanner[_]]

  private[this] final val TMP_DIR_NAME = "tmp"

  {
    logger.info("Storing data files at: " + storagePath)
    // Create directory if it does not exist
    RawUtils.createDirectory(storagePath)
    loadScannersFromDisk()
  }

  val tmpDir = {
    val t = storagePath.resolve(TMP_DIR_NAME)
    RawUtils.createDirectory(t)
    t
  }

  private[this] def loadScannersFromDisk(): Unit = {
    scanners.clear()
    val users = listUsers()
    logger.info("Found users: " + users)
    users.foreach(user => {
      val schemas = listUserSchemasFromDisk(user)
      logger.info("User: {}, Schemas: {}", user, schemas)
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
    val directories = RawUtils.listSubdirectories(userDataDir)
    directories.map(dir => dir.getFileName.toString).to[Set]
  }

  def listUsers(): Set[String] = {
    val directories = RawUtils.listSubdirectories(storagePath)
    // Ignore the special tmp directory, used for staging files.
    directories.filter(p => p.getFileName == TMP_DIR_NAME).map(dir => dir.getFileName.toString).toSet
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


  def registerSchema(schemaName: String, stagingDirectory: Path, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, stageDir: $stagingDirectory, user: $rawUser")
    val userDataDir = getUserStorageDir(rawUser)
    val finalDir = userDataDir.resolve(schemaName)

    logger.info(s"Moving to final destination: $finalDir")
    FileUtils.deleteDirectory(finalDir.toFile)
    Files.move(stagingDirectory, finalDir)

    val schema = loadSchema(rawUser, schemaName)
    val scanner = CodeGenerator.loadScanner(schemaName, schema)
    logger.info("Created scanner: " + scanner)
    scanners.put((rawUser, schemaName), scanner)
  }

  def loadSchema(rawUser: String, schemaName: String): RawSchema = {
    val schemaDir = getUserStorageDir(rawUser).resolve(schemaName)
    logger.info(s"Loading schema: $schemaName at directory: $schemaDir")

    val bp = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        if (u.isDirectory) return false
        val s = t.getFileName.toString
        s.startsWith(schemaName + ".")
      }
    }

    val iter = try {
      JavaConversions.asScalaIterator(Files.find(schemaDir, 1, bp).iterator())
    } catch {
      case ex: NoSuchFileException => throw new Exception("Corrupted storage directory. Could not find schema definition in directory: " + schemaDir, ex)
    }
    val list = iter.toList
    assert(list.size == 1, s"Expected one data file for schema: $schemaName in directory: $schemaDir. Found: $list.")

    val properties = schemaDir.resolve("properties.json")
    val userData = jsonMapper.readValue(properties.toFile, classOf[java.util.Map[String, Object]])

    RawSchema(schemaName, schemaDir.resolve("schema.xml"), new SchemaProperties(userData), list.head)
  }

  def listUserSchemas(rawUser: String): Seq[String] = {
    scanners.keys.filter(p => p._1 == rawUser).map(p => p._2).toSeq
  }

  def getScanner(rawUser: String, schemaName: String): RawScanner[_] = {
    logger.info(s"Getting scanner for $rawUser, $schemaName")
    scanners((rawUser, schemaName))
  }
}