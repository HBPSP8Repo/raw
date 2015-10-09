package raw.storage

import java.io.InputStream
import java.net.URI
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.function.BiPredicate

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.executor.{RawScanner, CodeGenerator, RawSchema, SchemaProperties}
import raw.utils.RawUtils
import scala.collection.mutable
import scala.collection.JavaConversions


class RawFile(val p: Path) extends RawResource {
  override def openInputStream(): InputStream = Files.newInputStream(p)

  override def toString = s"RawFile[${p.toString}]"

  override val fileName: String = p.getFileName.toString
}

class LocalStorageManager(val storagePath: Path = StorageManager.defaultDataDir) extends StorageManager with StrictLogging {
  private[this] final val TMP_DIR_NAME = "tmp"

  {
    logger.info("Storing data files at: " + storagePath)
    // Create directory if it does not exist
    RawUtils.createDirectory(storagePath)
  }

  override val stageDirectory = {
    val t = storagePath.resolve(TMP_DIR_NAME)
    logger.info(s"Staging directory: $t")
    RawUtils.createDirectory(t)
    t
  }

  //  override def listUsers(): List[String] = {
  //    val directories = RawUtils.listSubdirectories(storagePath)
  //    // Ignore the special tmp directory, used for staging files.
  //    directories.filter(p => !p.getFileName.toString.equals(TMP_DIR_NAME)).map(dir => dir.getFileName.toString).toList
  //  }

  override def registerSchema(schemaName: String, stagingDirectory: Path, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, stageDir: $stagingDirectory, user: $rawUser")
    val userDataDir = getUserStorageDir(rawUser)
    val finalDir = userDataDir.resolve(schemaName)

    logger.info(s"Moving to final destination: $finalDir")
    FileUtils.deleteDirectory(finalDir.toFile)
    Files.move(stagingDirectory, finalDir)

    val schema = loadSchemaFromStorage(rawUser, schemaName)
    val scanner = CodeGenerator.loadScanner(schemaName, schema)
    logger.info("Created scanner: " + scanner)
    scanners.put((rawUser, schemaName), scanner)
  }

  override protected[this] def listUsersFromStorage(): List[String] = {
    val directories = RawUtils.listSubdirectories(storagePath)
    // Ignore the special tmp directory, used for staging files.
    directories.filter(p => !p.getFileName.toString.equals(TMP_DIR_NAME)).map(dir => dir.getFileName.toString).toList
  }

  override protected[this] def listUserSchemasFromStorage(rawUser: String): List[String] = {
    val userDataDir = getUserStorageDir(rawUser)
    val directories = RawUtils.listSubdirectories(userDataDir)
    directories.filter(p => !p.getFileName.toString.equals(TMP_DIR_NAME)).map(dir => dir.getFileName.toString).toList
  }

  //
  //  def extractFilename(uri: URI): String = {
  //    val s = uri.getPath
  //    val startIndex = s.lastIndexOf("/") + 1
  //    s.substring(startIndex)
  //  }


  private[this] def getUserStorageDir(user: String): Path = {
    val userDataDir = storagePath.resolve(user)
    if (Files.notExists(userDataDir)) {
      RawUtils.createDirectory(userDataDir)
    }
    userDataDir
  }

  def loadSchemaFromStorage(rawUser: String, schemaName: String): RawSchema = {
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

    RawSchema(schemaName, new RawFile(schemaDir.resolve("schema.xml")), new SchemaProperties(userData), new RawFile(list.head))
  }

}