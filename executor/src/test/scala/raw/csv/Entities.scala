package raw.csv

import raw.util.CSVParser

import scala.math.Ordered.orderingToOrdered

/* About Ordered[] using tuples:
 * http://stackoverflow.com/questions/19345030/easy-idiomatic-way-to-define-ordering-for-a-simple-case-class
 */
case class Student(name: String, birthYear: Int, office: String, department: String) extends Ordered[Student] {
  override def compare(that: Student): Int =
    (name, birthYear, office, department).compare(that.name, that.birthYear, that.office, that.department)
}

case class Professor(name: String, office: String) extends Ordered[Professor] {
  override def compare(that: Professor): Int =
    (name, office).compare(that.name, that.office)
}

case class Department(name: String, discipline: String, prof: String) extends Ordered[Department] {
  override def compare(that: Department): Int =
    (name, discipline, prof).compare(that.name, that.discipline, that.prof)
}

object ReferenceTestData {
  val students: List[Student] = CSVParser("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val profs: List[Professor] = CSVParser("data/profs.csv", l => Professor(l(0), l(1)))
  val departments: List[Department] = CSVParser("data/departments.csv", l => Department(l(0), l(1), l(2)))
}