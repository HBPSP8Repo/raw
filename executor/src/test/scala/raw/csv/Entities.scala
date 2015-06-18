package raw.csv

import raw.util.CSVParser

case class Publication(title: String, authors: List[String])

case class Student(name: String, birthYear: Int, office: String, department: String)

case class Professor(name: String, office: String)

case class Department(name: String, discipline: String, prof: String)

object ReferenceTestData {
  val students: List[Student] = CSVParser("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val profs: List[Professor] = CSVParser("data/profs.csv", l => Professor(l(0), l(1)))
  val departments: List[Department] = CSVParser("data/departments.csv", l => Department(l(0), l(1), l(2)))
}