package raw.datasets.patients

import org.apache.spark.SparkContext
import raw.datasets.Dataset

case class Patient(city: String, country: String, patient_id: String,
                   year_of_birth: Int, gender: String, diagnosis: Seq[Diagnostic])

case class Diagnostic(diag_id: String, code: String, diag_date: String,
                      description: String, patient_id: String)

object Patients {
  def loadPatients(sc: SparkContext) = List(
    new Dataset[Patient]("patients", "data/patients/patients.json", sc))
}