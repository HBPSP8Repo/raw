package raw

import java.nio.file.{Path, Paths}

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import raw.datasets.{Author, Patient, Publication}
import raw.executor.{RawScanner, RawSchema}

import scala.collection.mutable


object TestScanners extends StrictLogging {
  val scanners = new mutable.ListBuffer[RawScanner[_]]()

  val publications: RawScanner[Publication] = createScanner[Publication]("data/publications/publications.json")
  val authors: RawScanner[Author] = createScanner[Author]("data/publications/authors.json")
  val authorsSmall: RawScanner[Author] = createScanner[Author]("data/publications/authorsSmall.json")
  val publicationsSmall: RawScanner[Publication] = createScanner[Publication]("data/publications/publicationsSmall.json")
  val publicationsSmallWithDups: RawScanner[Publication] = createScanner[Publication]("data/publications/publicationsSmallWithDups.json")
  val patients: RawScanner[Patient] = createScanner[Patient]("data/patients/patients.json")

  private[this] def getSchemaName(p: Path): String = {
    val fileName = p.getFileName.toString
    val dotIndex = fileName.lastIndexOf('.')
    fileName.substring(0, dotIndex)
  }

  def createScanner[T: Manifest](dataFileResource: String): RawScanner[T] = {
    val p = Paths.get(Resources.getResource(dataFileResource).toURI)
    val schemaName = getSchemaName(p)
    val schema = new RawSchema(schemaName, null, null, p)
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
