package raw.executor

import com.typesafe.scalalogging.StrictLogging
import raw.datasets.AccessPath

object RawServer extends StrictLogging {

  def registerSchema(schemaName: String, dataDir: String, user: String): Unit = {
    StorageManager.registerSchema2(schemaName, dataDir, user)
  }
  
  def doQuery(query: String, rawUser: String): String = {
    // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
    // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
    // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
    // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
    val cleanedQuery = query.trim.replaceAll("\\s+", " ")

    val schemas: Set[String] = StorageManager.listUserSchemas(rawUser)
    logger.info("Found schemas: " + schemas.mkString(", "))
    val scanners: Set[RawScanner[_]] = schemas.map(name => StorageManager.getScanner(rawUser, name))
    CodeGenerator.query2(cleanedQuery, scanners)
  }

}
