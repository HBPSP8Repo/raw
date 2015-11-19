package raw

import raw.datasets.{Author, Patient, Publication}
import raw.executor.{RawScanner, SparkRawScanner}

import scala.collection.mutable
import scala.reflect._
import scala.reflect.runtime.universe._

abstract class AbstractSparkTest extends AbstractRawTest with SharedSparkContext {
//  val scanners = new mutable.ListBuffer[SparkRawScanner[_]]()

  // Initialize once the SparkContext is created.
//  var publications: SparkRawScanner[Publication] = _
//  var authors: SparkRawScanner[Author] = _
//  var authorsSmall: SparkRawScanner[Author] = _
//  var publicationsSmall: SparkRawScanner[Publication] = _
//  var publicationsSmallWithDups: SparkRawScanner[Publication] = _
//  var patients: SparkRawScanner[Patient] = _
//  var httpLogs: SparkRawScanner[String] = _
//
//  def createSparkScanner[T: ClassTag : TypeTag](scanner: RawScanner[T]): SparkRawScanner[T] = {
//    val sparkScanner = new SparkRawScanner(scanner, sc)
//    scanners += sparkScanner
//    sparkScanner
//  }
//
//  override def beforeAll() {
//    super.beforeAll()
//    try {
//      publications = createSparkScanner(TestDatasources.publications)
//      authors = createSparkScanner(TestDatasources.authors)
//      authorsSmall = createSparkScanner(TestDatasources.authorsSmall)
//      publicationsSmall = createSparkScanner(TestDatasources.publicationsSmall)
//      publicationsSmallWithDups = createSparkScanner(TestDatasources.publicationsSmallWithDups)
//      patients = createSparkScanner(TestDatasources.patients)
//      httpLogs = createSparkScanner(TestDatasources.httpLogs)
//    } catch {
//      case ex: Exception => super.afterAll()
//    }
//  }
}
