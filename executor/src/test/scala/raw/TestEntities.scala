package raw.datasets

//
// Patients dataset
//

case class Patient(patient_id: String,
                  name: String,
                  surname: String,
                  birthDate: String,
                  gender: String,
                  city: String,
                  country: String,
                  patientBasedData: PatientBasedData)

case class PatientBasedData(formData: Seq[FormData],
                           diagnosis: Seq[Diagnostic],
                           stays: Seq[Stay])

case class FormData(formType: String,
                   FormID: Int,
                   items: Seq[Item])

case class Item(name: String,
               value: String)

case class Diagnostic(diag_id: String,
                      diag_date: String,
                      code: String,
                      description: String,
                      patient_id: String)

case class Stay(wardId: Int,
                roomNumber: Int,
                bedNumber: Int,
                validFrom: String,
                validUntill: String)

//
// Publications dataset
//

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Author(name: String, title: String, year: Int)
