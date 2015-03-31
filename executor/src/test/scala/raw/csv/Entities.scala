package raw.csv

import scala.math.Ordered.orderingToOrdered

case class Student(name: String, birthYear: Int, office: String, department: String) extends Ordered[Student] {
  override def compare(that: Student): Int = (name, birthYear, office, department).compareTo(that.name, that.birthYear, that.office, that.department)
}

case class Professor(name: String, office: String) extends Ordered[Professor] {
  override def compare(that: Professor): Int = (name, office).compareTo(that.name, that.office)
}

case class Department(name: String, discipline: String, prof: String) extends Ordered[Department] {
  override def compare(that: Department): Int = (name, discipline, prof).compareTo(that.name, that.discipline, that.prof)
}
