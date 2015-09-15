package raw.executor

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import raw.utils.RawUtils

import scala.collection.JavaConversions

object StorageManager extends StrictLogging {
  val storagePath = {
    val rawDir = ConfigFactory.load().getString("raw.datadir")
    logger.info("Storing data files at: " + rawDir)
    val t = Paths.get(rawDir)
    RawUtils.createDirectory(t)
    t
  }

  def registerSchema(schemaName: String, xmlSchema: String, fileURI: String, rawUser: String) = {
    logger.info(s"Registering schema: $schemaName, file: $fileURI, user: $rawUser")
    val uri = new URI(fileURI)
    // user data storage
    val userDataDir = storagePath.resolve(rawUser)
    RawUtils.createDirectory(userDataDir)
    val dataFilePath = userDataDir.resolve(schemaName + ".json")

    val schemaPath = userDataDir.resolve(schemaName + ".xml")
    logger.info(s"Writing schema: $schemaPath")
    Files.write(schemaPath, xmlSchema.getBytes(StandardCharsets.UTF_8))

    logger.info(s"Wrting data: $dataFilePath")
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
  }

  def listSchemas(rawUser: String): Seq[String] = {
    val userDir = storagePath.resolve(rawUser)
    val iter = JavaConversions.asScalaIterator(Files.list(userDir).iterator())
    val schemas: Iterator[String] = iter
      .filter(p => p.getFileName.toString.endsWith(".xml"))
      .map(p => p.getFileName.toString.dropRight(4))
    schemas.toSeq
  }

  case class RawSchema(schemaFile: Path, dataFile: Path)

  def getSchema(rawUser: String, schema: String): RawSchema = {
    val userDir = storagePath.resolve(rawUser)
    RawSchema(userDir.resolve(schema + ".xml"), userDir.resolve(schema + ".json"))
  }
}
