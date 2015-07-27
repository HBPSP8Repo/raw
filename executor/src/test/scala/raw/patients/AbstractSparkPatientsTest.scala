package raw.patients

import java.nio.file.Paths

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.datasets.patients.{Patient, Patients}
import raw.datasets.{AccessPath, Dataset}
import raw.executionserver.ResultConverter
import raw.perf.QueryCompilerClient
import raw.{LDBDockerContainer, SharedSparkContext}

import scala.reflect._

abstract class AbstractSparkPatientsTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter
  with LDBDockerContainer {

  var queryCompiler: QueryCompilerClient = _
  var patientsDS: List[Dataset[_]] = _
  var accessPaths: List[AccessPath[_]] = _
  var patientsRDD: RDD[Patient] = _

  override def beforeAll() {
    super.beforeAll()
    try {
      queryCompiler = new QueryCompilerClient(rawClassLoader)
      patientsDS = Patients.loadPatients(sc)
      patientsRDD = patientsDS.filter(ds => ds.accessPath.tag == classTag[Patient]).head.rdd.asInstanceOf[RDD[Patient]]
      accessPaths = patientsDS.map(ds => ds.accessPath)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
