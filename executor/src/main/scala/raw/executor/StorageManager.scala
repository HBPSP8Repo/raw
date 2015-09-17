package raw.executor

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.function.BiPredicate

import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw.utils.RawUtils

import scala.collection.JavaConversions

object StorageManager extends StrictLogging {
  final val schemaExtension = ".schema.xml"
  final val defaultDataDir = RawUtils.getTemporaryDirectory().resolve("rawschemas")

  val storagePath = {
    val rawDir = try {
      Paths.get(ConfigFactory.load().getString("raw.datadir"))
    } catch {
      case ex: ConfigException.Missing => defaultDataDir
    }
    logger.info("Storing data files at: " + rawDir)
    RawUtils.createDirectory(rawDir)
    rawDir
  }

  def extractFilename(uri: URI): String = {
    val s = uri.getPath
    val startIndex = s.lastIndexOf("/") + 1
    s.substring(startIndex)
  }

  def registerSchema(schemaName: String, xmlSchema: String, dataFileURI: String, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, file: $dataFileURI, user: $rawUser")
    val uri = new URI(dataFileURI)
    // Initialize the user storage directory
    val userDataDir = storagePath.resolve(rawUser)
    RawUtils.createDirectory(userDataDir)

    /* Use a temporary directory to store the schema and the data file. This way, when overwriting a schema, we don't
     * delete the previous version of a schema until the new one is safely written on disk
     */
    val tmpDir = Files.createTempDirectory("rawdata")

    val schemaPath = tmpDir.resolve(schemaName + schemaExtension)
    logger.info(s"Writing schema: $schemaPath")
    Files.write(schemaPath, xmlSchema.getBytes(StandardCharsets.UTF_8))

    val dataFilePath = tmpDir.resolve(extractFilename(uri))
    logger.info(s"Writing data: $dataFilePath")
    val localFile = if (uri.getScheme().startsWith("http")) {
      logger.info("toURL: " + uri.toURL)
      val is = uri.toURL.openStream()
      Files.copy(is, dataFilePath, StandardCopyOption.REPLACE_EXISTING)
      is.close()
      dataFilePath
    } else {
      val src = Paths.get(uri)
      Files.copy(src, dataFilePath, StandardCopyOption.REPLACE_EXISTING)
    }

    val finalDir = userDataDir.resolve(schemaName)
    logger.info(s"Moving to final destination: $finalDir")
    FileUtils.deleteDirectory(finalDir.toFile)
    Files.move(tmpDir, finalDir)
  }

  def listSchemas(rawUser: String): Seq[String] = {
    val userDir = storagePath.resolve(rawUser)
    val bp = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        t.toString.endsWith(schemaExtension)
      }
    }
    val iter = JavaConversions.asScalaIterator(Files.find(userDir, 5, bp).iterator())
    val schemas: Iterator[String] = iter.map(p => p.getFileName.toString.dropRight(schemaExtension.size))
    schemas.toSeq
  }

  case class RawSchema(schemaFile: Path, dataFile: Path)


  def getSchema(rawUser: String, schema: String): RawSchema = {
    val schemaDir = storagePath.resolve(rawUser).resolve(schema)

    val bp = new BiPredicate[Path, BasicFileAttributes] {
      override def test(t: Path, u: BasicFileAttributes): Boolean = {
        val s = t.toString
        s.endsWith(".csv") || s.endsWith(".json")
      }
    }
    val iter = JavaConversions.asScalaIterator(Files.find(schemaDir, 1, bp).iterator())
    val list = iter.toList
    assert(list.size == 1, "Expected one data file. Found: " + list)
    RawSchema(schemaDir.resolve(schema + schemaExtension), list.head)
  }
}
