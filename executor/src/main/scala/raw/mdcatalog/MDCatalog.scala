package raw.mdcatalog

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

object MDCatalog extends StrictLogging {
  private[this] val catalog = new mutable.HashMap[(String, String), DataSource]()

  // Hack for running unit tests: SBT reuses the same JVM to run several test classes, so we have to clear
  // the contents of the MDCatalog whenever starting a new RawServer.
  def clear(): Unit = {
    catalog.clear()
  }

  // TODO: Manage user information
  def register(user: String, schemaName: String, dataSource:DataSource): Unit = {
    logger.info(s"Registered datasource: ($user, $schemaName) -> $dataSource")
    catalog.put((user, schemaName), dataSource)
  }

  def lookup(user: String, schemaName: String): Option[DataSource] = {
    catalog.get((user, schemaName))
  }

  def listUserSchemas(user: String): Seq[DataSource] = {
    logger.info(s"Looking up schemas for user: $user. Catalog: ${catalog.keySet}")
    val schemas = catalog
      .filter({ case ((u, _), _) => u == user })
      .map({ case ((_, _), ds) => ds })
      .toSeq
    logger.info(s"Found schemas: ${schemas.map(ds => ds.name)}")
    schemas
  }
}
