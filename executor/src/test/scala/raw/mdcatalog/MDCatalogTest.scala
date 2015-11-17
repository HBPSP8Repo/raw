package raw.mdcatalog

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestScanners
import raw.executor.InferrerConfiguration

class MDCatalogTest extends FunSuite with StrictLogging with BeforeAndAfterAll with InferrerConfiguration {

  test("register local file json") {
    val source: DataSource = DataSource.createLocalDataSource("patients", TestScanners.patientsPath)

    logger.info(s"Source: $source")
    MDCatalog.register("patients", source)

    MDCatalog.lookup("patients") match {
      case Some(DataSource(name, tipe, accessPaths)) => logger.info(s"Type: $tipe\nAccessPaths: $accessPaths")
      case a@_ => fail(s"Unexpected data source: $a")
    }
  }

  test("register local file csv") {
    val source: DataSource = DataSource.createLocalDataSource("students", TestScanners.studentsPath)
    logger.info(s"Source: $source")
    MDCatalog.register("students", source)

    MDCatalog.lookup("students") match {
      case Some(DataSource(name, tipe, accessPaths)) => logger.info(s"Type: $tipe\nAccessPaths: $accessPaths")
      case a@_ => fail(s"Unexpected data source: $a")
    }
  }
}
