package raw.patients

import org.apache.spark.rdd.RDD
import raw.AbstractSparkTest
import raw.datasets.patients.{Patient, Patients}

import scala.reflect.runtime.universe._

abstract class AbstractSparkPatientsTest extends AbstractSparkTest(Patients.Spark.patients) {

  var patientsRDD: RDD[Patient] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      patientsRDD = accessPaths.filter(ap => ap.tag == typeTag[Patient]).head.path.asInstanceOf[RDD[Patient]]
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
