package raw.datasets.patients

import org.apache.spark.SparkContext
import raw.datasets.{AccessPath, Dataset}
import scala.reflect.runtime.universe._


case class Patient(city: String, country: String, patient_id: String,
                   year_of_birth: Int, gender: String, diagnosis: Seq[Diagnostic])

case class Diagnostic(diag_id: String, code: String, diag_date: String,
                      description: String, patient_id: String)

object Patients {
  val patientsDS = Dataset("patients", "data/patients/patients.json", typeTag[Patient])

  object Spark {
    def patientsAP(sc: SparkContext) = AccessPath.toRDDAcessPath(patientsDS, sc)
    def patients(sc: SparkContext) = List(patientsAP(sc))
  }

  object Scala {
    def patientsAP() = AccessPath.toScalaAcessPath(patientsDS)
    def patients() = List(patientsAP())
  }
}
