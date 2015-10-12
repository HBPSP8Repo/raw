package raw

import java.nio.file.{Path, Paths}

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import raw.datasets.{Author, Patient, Publication}
import raw.executor.{RawScanner, RawSchema}
import raw.storage.RawLocalFile
import raw.utils.RawUtils

import scala.collection.mutable


object TestScanners extends StrictLogging {
  val scanners = new mutable.ListBuffer[RawScanner[_]]()

  val publicationsPath = RawUtils.toPath("data/publications/publications.json")
  val authorsPath = RawUtils.toPath("data/publications/authors.json")
  val authorsSmallPath = RawUtils.toPath("data/publications/authorsSmall.json")
  val publicationsSmallPath = RawUtils.toPath("data/publications/publicationsSmall.json")
  val publicationsSmallWithDupsPath = RawUtils.toPath("data/publications/publicationsSmallWithDups.json")
  val patientsPath = RawUtils.toPath("data/patients/patients.json")
  val studentsPath = RawUtils.toPath("data/students/students.csv")
  val studentsNoHeaderPath = RawUtils.toPath("data/students/students_no_header.csv")

  lazy val publications: RawScanner[Publication] = createScanner[Publication](publicationsPath)
  lazy val authors: RawScanner[Author] = createScanner[Author](authorsPath)
  lazy val authorsSmall: RawScanner[Author] = createScanner[Author](authorsSmallPath)
  lazy val publicationsSmall: RawScanner[Publication] = createScanner[Publication](publicationsPath)
  lazy val publicationsSmallWithDups: RawScanner[Publication] = createScanner[Publication](publicationsSmallWithDupsPath)
  lazy val patients: RawScanner[Patient] = createScanner[Patient](patientsPath)

  private[this] def getSchemaName(p: Path): String = {
    val fileName = p.getFileName.toString
    val dotIndex = fileName.lastIndexOf('.')
    fileName.substring(0, dotIndex)
  }


  def createScanner[T: Manifest](p: Path): RawScanner[T] = {
    val schemaName = getSchemaName(p)
    val schema = new RawSchema(schemaName, null, null, new RawLocalFile(p))
    //    val scanner = RawScanner(schema, manifest[T])
    val scanner = RawScanner[T](schema)
    scanners += scanner
    logger.info("Scanner: " + scanner)
    scanner
  }
}

abstract class AbstractScalaTest extends AbstractRawTest {
  val scanners = TestScanners.scanners
}
