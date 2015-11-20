package raw.storage

import java.io.InputStream
import java.nio.file.Path

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import raw.executor.{CodeGenerator, RawScanner}
import raw.mdcatalog.DataSource
import raw.utils.RawUtils

import scala.collection.mutable

/* Represents a data file for Raw, either an S3 file or a local file. Used to read the file, regardless of its
 * source. */
abstract class RawResource() {
  val fileName: String

  def openInputStream(): InputStream
}

/* Type-safe representation of the type of storage backend. */
sealed trait StorageBackend

case object S3StorageBackend extends StorageBackend

case object LocalStorageBackend extends StorageBackend

object StorageBackend {
  def apply(backendConfigOption: String): StorageBackend = {
    backendConfigOption.toLowerCase match {
      case "local" => LocalStorageBackend
      case "s3" => S3StorageBackend
      case _ => throw new IllegalArgumentException("Unknown storage backend: " + backendConfigOption)
    }
  }
}


object StorageManager {
  val defaultDataDir = RawUtils.getTemporaryDirectory().resolve("rawstage")
}

/** Base functionality shared by all Storage managers. This class keeps a cache with the schema informatin in the
scanners map. It exposes several methods that use this cache to list users, list schemas and obtain a scanner to traverse
 over the data files. There is another set of methods to do these operations by querying the underlying storage backend
 (the XXXFromStorage() methods). Subclasses must implement these methods.
  */
abstract class StorageManager extends StrictLogging {
  protected final val jsonMapper = new ObjectMapper()

  // Map from (user, schema) -> schemaScanner. This caches in memory the schema information.
  protected[this] val scanners: mutable.HashMap[(String, String), RawScanner[_]] = new mutable.HashMap[(String, String), RawScanner[_]]()

  /* Reloads the schema information from storage (ie., rebuilds the scanners map.*/
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

  /** List of known users */
  def listUsers(): List[String] = {
    scanners.keys.map(p => p._2).toSet.toList
  }

  /** List of schemas belonging to an user */
  def listUserSchemas(rawUser: String): List[String] = {
    scanners.keys.filter(p => p._1 == rawUser).map(p => p._2).toList
  }

  /** Gets the scanner for the given schema */
  def getScanner(rawUser: String, schemaName: String): RawScanner[_] = {
    scanners((rawUser, schemaName))
  }

  /*
   * Abstract methods, implement in subclasses
   */

  /** where to store the temporary files used while registering a schema. */
  val stageDirectory: Path

  /** Loads the list of users from the storage backend */
  protected[this] def listUsersFromStorage(): List[String]

  protected[this] def listUserSchemasFromStorage(user: String): List[String]

  def loadSchemaFromStorage(user: String, schemaName: String): DataSource

  def registerSchema(schemaName: String, stagingDirectory: Path, rawUser: String)
}