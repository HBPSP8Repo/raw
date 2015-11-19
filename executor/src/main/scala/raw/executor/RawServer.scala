package raw.executor

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import raw.QueryLanguages.QueryLanguage
import raw.RawQuery
import raw.mdcatalog.MDCatalog
import raw.storage._

class RawServer(storageDir: Path, storageBackend: StorageBackend) extends StrictLogging {

  // Classloader to be used as parent of Spark's classloader and for loading the classes compiled ar runtime.
  val rawClassloader = new RawMutableURLClassLoader(getClass.getClassLoader)
  // Workaround for testing: SBT reuses the same JVM, so we must clear any state in this singleton
  MDCatalog.clear()
  private[this] val queryCompiler = new RawCompiler(rawClassloader)

  val storageManager: StorageManager = storageBackend match {
    case LocalStorageBackend => new LocalStorageManager(storageDir)
    case S3StorageBackend => new S3StorageManager(storageDir)
  }
  storageManager.loadFromStorage()

  def registerSchema(schemaName: String, stagingDirectory: Path, user: String): Unit = {
    storageManager.registerSchema(schemaName, stagingDirectory, user)
  }

  def doQueryStart(queryLanguage: QueryLanguage, query: String, rawUser: String): RawQuery = {
    doQuery(queryLanguage, query, rawUser)
  }

  def getSchemas(user: String): Seq[String] = {
    MDCatalog.listUserSchemas(user).map(ds => ds.name)
  }

  def doQuery(queryLanguage: QueryLanguage, query: String, rawUser: String): RawQuery = {
    // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
    // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
    // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
    // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
    //    val cleanedQuery = query.trim.replaceAll("\\s+", " ")
    queryCompiler.compile(queryLanguage, query, rawUser)
  }
}
