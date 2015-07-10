package raw.patients

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.SharedSparkContext
import raw.datasets.patients.{Patient, PatientsDataset}
import raw.executionserver.{AccessPath, ResultConverter}

abstract class AbstractSparkPatientsTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter {

  var pubsDS: PatientsDataset = _
  var accessPaths: List[AccessPath[_]] = _
  var patientsRDD: RDD[Patient] = _

  override def beforeAll() {
    super.beforeAll()
    pubsDS = new PatientsDataset(sc)
    patientsRDD = pubsDS.patientsRDD
    accessPaths = pubsDS.accessPaths
  }
}
