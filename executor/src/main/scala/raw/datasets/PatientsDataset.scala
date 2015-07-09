package raw.datasets.patients

import org.apache.spark.SparkContext
import raw.executionserver.{AccessPath, Dataset, DefaultSparkConfiguration, JsonLoader}

import scala.reflect._

case class Patient(city: String, country: String, patient_id: String, year_of_birth: Int, gender: String, diagnosis: Seq[Diagnostic])

case class Diagnostic(diag_id: String, code: String, diag_date: String, description: String, patient_id: String)

class PatientsDataset(sc: SparkContext) extends Dataset {
  val patients: List[Patient] = JsonLoader.load[Array[Patient]]("data/patients/patients.json").head.toList

  val patientsRDD = DefaultSparkConfiguration.newRDDFromJSON[Patient](patients, sc)

  val accessPaths: List[AccessPath[_]] =
    List(
      AccessPath("patients", patientsRDD, classTag[Patient])
    )
}
