package raw

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfterAll
import raw.executor.InferrerConfiguration
import raw.mdcatalog.{DataSource, MDCatalog}
import raw.utils.RawUtils

import scala.collection.mutable


object TestDatasources extends StrictLogging {
  val datasources = new mutable.ListBuffer[DataSource]()

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

  val publications: DataSource = createDatasource(publicationsPath)
  val authors: DataSource = createDatasource(authorsPath)
  val authorsSmall: DataSource = createDatasource(authorsSmallPath)
  val publicationsSmall: DataSource = createDatasource(publicationsSmallPath)
  val publicationsSmallWithDups: DataSource = createDatasource(publicationsSmallWithDupsPath)
  val patients: DataSource = createDatasource(patientsPath)
  val httpLogs: DataSource = createDatasource(httpLogsPath)

  private[this] def getSchemaName(p: Path): String = {
    val fileName = p.getFileName.toString
    val dotIndex = fileName.lastIndexOf('.')
    if (dotIndex > 0)
      fileName.substring(0, dotIndex)
    else
      fileName
  }


  def createDatasource(p: Path): DataSource = {
    val schemaName = getSchemaName(p)
    val ds = DataSource.createLocalDataSource(schemaName, p)
    datasources += ds
    ds
  }
}

abstract class AbstractScalaTest extends AbstractRawTest with InferrerConfiguration with BeforeAndAfterAll {
  def executeTestQuery(query: String): Any = {
    queryCompiler.compile(QueryLanguages.Qrawl, query, unitTestUser).computeResult
  }

  override def beforeAll() {
    super.beforeAll()
    try {
      TestDatasources.datasources.foreach(ds => MDCatalog.register(unitTestUser, ds.name, ds))
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }


}
