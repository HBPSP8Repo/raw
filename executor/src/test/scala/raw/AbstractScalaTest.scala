package raw

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import raw.datasets._
import raw.executor.{InferrerConfiguration, RawScanner}
import raw.mdcatalog.DataSource
import raw.utils.RawUtils

import scala.collection.mutable


object TestScanners extends StrictLogging {
  val scanners = new mutable.ListBuffer[RawScanner[_]]()

  logger.info("Creating test scanners")
  val publicationsPath = RawUtils.toPath("data/publications/publications.json")
  val authorsPath = RawUtils.toPath("data/publications/authors.json")
  val authorsSmallPath = RawUtils.toPath("data/publications/authorsSmall.json")
  val publicationsSmallPath = RawUtils.toPath("data/publications/publicationsSmall.json")
  val publicationsSmallWithDupsPath = RawUtils.toPath("data/publications/publicationsSmallWithDups.json")
  val patientsPath = RawUtils.toPath("data/patients/patients.json")
  val studentsPath = RawUtils.toPath("data/students/students.csv")
  val studentsNoHeaderPath = RawUtils.toPath("data/students/students_no_header.csv")
  val httpLogsPath = RawUtils.toPath("data/httplogs/NASA_access_log_Aug95_small")
  val httpLogsPathUTF8 = RawUtils.toPath("data/httplogs/NASA_access_log_Aug95_small_utf8")

  val publications: RawScanner[Publication] = createScanner[Publication](publicationsPath)
  val authors: RawScanner[Author] = createScanner[Author](authorsPath)
  val authorsSmall: RawScanner[Author] = createScanner[Author](authorsSmallPath)
  val publicationsSmall: RawScanner[Publication] = createScanner[Publication](publicationsSmallPath)
  val publicationsSmallWithDups: RawScanner[Publication] = createScanner[Publication](publicationsSmallWithDupsPath)
  val patients: RawScanner[Patient] = createScanner[Patient](patientsPath)
  val httpLogs: RawScanner[String] = createScanner[String](httpLogsPath)

  private[this] def getSchemaName(p: Path): String = {
    val fileName = p.getFileName.toString
    val dotIndex = fileName.lastIndexOf('.')
    if (dotIndex > 0)
      fileName.substring(0, dotIndex)
    else
      fileName
  }


  def createScanner[T: Manifest](p: Path): RawScanner[T] = {
    val schemaName = getSchemaName(p)
    val dataSource = DataSource.createLocalDataSource(schemaName, p)
    //    val schema = new RawSchema(schemaName, null, null, new RawLocalFile(p))
    //    val scanner = RawScanner(schema, manifest[T])
    val scanner = RawScanner[T](dataSource)
    scanners += scanner
    logger.info(s"Created: $scanner")
    scanner
  }
}

abstract class AbstractScalaTest extends AbstractRawTest with InferrerConfiguration {
  val scanners = TestScanners.scanners
}
