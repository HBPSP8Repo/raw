package raw.patients

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.SharedSparkContext
import raw.datasets.patients.{Patient, Patients}
import raw.datasets.{AccessPath, Dataset}
import raw.executionserver.ResultConverter

import scala.reflect._

abstract class AbstractSparkPatientsTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter {

  var pubsDS: List[Dataset[_]] = _
  var accessPaths: List[AccessPath[_]] = _
  var patientsRDD: RDD[Patient] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      pubsDS = Patients.loadPatients(sc)
      patientsRDD = pubsDS.filter(ds => ds.accessPath.tag == classTag[Patient]).head.rdd.asInstanceOf[RDD[Patient]]
      accessPaths = pubsDS.map(ds => ds.accessPath)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
