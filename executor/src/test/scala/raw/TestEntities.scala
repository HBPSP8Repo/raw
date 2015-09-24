package raw.datasets

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Patient(city: String, country: String, patient_id: String,
                   year_of_birth: Int, gender: String, diagnosis: Seq[Diagnostic])

case class Diagnostic(diag_id: String, code: String, diag_date: String,
                      description: String, patient_id: String)

case class Author(name: String, title: String, year: Int)
