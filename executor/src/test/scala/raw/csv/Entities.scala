package raw.csv

case class Student(name: String, birthYear: Int, office: String, department: String)

case class Professor(name: String, office: String)

case class Department(name: String, discipline: String, prof: String)
