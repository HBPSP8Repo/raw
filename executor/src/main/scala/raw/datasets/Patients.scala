package raw.datasets.patients

import org.apache.spark.SparkContext
import raw.datasets.AccessPath

case class Patient(city: String, country: String, patient_id: String,
                   year_of_birth: Int, gender: String, diagnosis: Seq[Diagnostic])

case class Diagnostic(diag_id: String, code: String, diag_date: String,
                      description: String, patient_id: String)

object Patients {
  def loadPatients_(sc: SparkContext) = AccessPath.loadJSON[Patient]("patients", "data/patients/patients.json", sc)

  def loadPatients(sc: SparkContext) = List(loadPatients_(sc))
}