package raw.executor

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import raw.QueryLanguages.QueryLanguage
import raw.RawQuery
import raw.storage._

class RawServer(storageDir: Path, storageBackend: StorageBackend) extends StrictLogging {

  val storageManager: StorageManager = storageBackend match {
    case LocalStorageBackend => new LocalStorageManager(storageDir)
    case S3StorageBackend => new S3StorageManager(storageDir)
  }
  storageManager.loadFromStorage()

  def registerSchema(schemaName: String, stagingDirectory: Path, user: String): Unit = {
    storageManager.registerSchema(schemaName, stagingDirectory, user)
  }

  def doQueryStart(queryLanguage: QueryLanguage, query: String, rawUser: String): RawQuery = {
    _doQuery(queryLanguage, query, rawUser)
  }

  def doQuery(queryLanguage: QueryLanguage, query: String, rawUser: String): Any = {
    val compiledQuery = _doQuery(queryLanguage, query, rawUser)
    compiledQuery.computeResult
  }

  def getSchemas(user: String): Seq[String] = {
    storageManager.listUserSchemas(user)
  }

  private[this] def _doQuery(queryLanguage: QueryLanguage, query: String, rawUser: String): RawQuery = {
    // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
    // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
    // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
    // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
    //    val cleanedQuery = query.trim.replaceAll("\\s+", " ")
    val schemas: Seq[String] = storageManager.listUserSchemas(rawUser)
    val scanners: Seq[RawScanner[_]] = schemas.map(name => storageManager.getScanner(rawUser, name))
    CodeGenerator.compileQuery(queryLanguage, query, scanners)
  }
}
