package raw.storage

import java.io.InputStream
import java.nio.file.Path

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import raw.executor.{CodeGenerator, SchemaProperties, RawScanner, RawSchema}
import raw.utils.RawUtils

import scala.collection.mutable

abstract class RawResource() {
  val fileName: String

  def openInputStream(): InputStream
}

object StorageManager {
  val defaultDataDir = RawUtils.getTemporaryDirectory().resolve("rawstage")
}


object StorageBackend {
  def apply(backendConfigOption: String): StorageBackend = {
    backendConfigOption.toLowerCase match {
      case "local" => LocalStorageBackend
      case "s3" => S3StorageBackend
      case _ => throw new IllegalArgumentException("Unknown storage backend: " + backendConfigOption)
    }
  }
}

sealed trait StorageBackend

case object S3StorageBackend extends StorageBackend

case object LocalStorageBackend extends StorageBackend


abstract class StorageManager extends StrictLogging {
  protected final val jsonMapper = new ObjectMapper()

  // Map from (user, schema) -> schemaScanner
  protected[this] val scanners: mutable.HashMap[(String, String), RawScanner[_]] = new mutable.HashMap[(String, String), RawScanner[_]]()

  def loadFromStorage(): Unit = {
    scanners.clear()
    val users = listUsersFromStorage()
    logger.info("Found users: " + users)
    users.foreach(user => {
      val schemas = listUserSchemasFromStorage(user)
      logger.info("User: {}, Schemas: {}", user, schemas)
      schemas.foreach(schemaName => {
        val schema = loadSchemaFromStorage(user, schemaName)
        val scanner = CodeGenerator.loadScanner(schemaName, schema)
        logger.info("Created scanner: " + scanner)
        scanners.put((user, schemaName), scanner)
      })
    }
    )
  }

  val stageDirectory: Path

  protected[this] def listUsersFromStorage(): List[String]

  protected[this] def listUserSchemasFromStorage(user: String): List[String]

  def loadSchemaFromStorage(user: String, schemaName: String): RawSchema

  def registerSchema(schemaName: String, stagingDirectory: Path, rawUser: String)

  def listUsers(): List[String] = {
    scanners.keys.map(p => p._2).toSet.toList
  }

  def listUserSchemas(rawUser: String): List[String] = {
    scanners.keys.filter(p => p._1 == rawUser).map(p => p._2).toList
  }

  def getScanner(rawUser: String, schemaName: String): RawScanner[_] = {
    logger.info(s"Getting scanner for $rawUser, $schemaName")
    scanners((rawUser, schemaName))
  }
}