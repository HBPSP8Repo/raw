//package raw
//package executor
//package reference
//
//import shapeless.HMap
//
//object Example extends App {
//
//  import algebra._
//  import Expressions._
//  import LogicalAlgebra._
//
//  println("Hello World")
//
//  case class Student(name: String, age: Int)
//  def students = List(Student("Miguel", 18), Student("Ben", 21))
//
//  case class Teacher(title: String, firstName: String)
//  def teachers = List(Teacher("Prof.", "Natassa"), Teacher("Dr", "Cesar"))
//
//  implicit val stringToListOfStudent = new RawSources[String, List[Student]]
//  implicit val stringToListOfTeachers = new RawSources[String, List[Teacher]]
//
//  val result = Raw.query("for (s <- Students; s.age > 20) yield list s", HMap[RawSources]("Students" -> students, "Teachers" -> teachers))
//  println(result)
//
//  println("Done")
//}
