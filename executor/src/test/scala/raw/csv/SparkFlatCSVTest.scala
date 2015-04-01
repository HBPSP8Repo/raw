package raw.csv

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.Raw
import raw.util.CSVToRDDParser
import shapeless.HList

class SparkFlatCSVTest extends FunSuite with LazyLogging with BeforeAndAfterAll {
  val csvReader = new CSVToRDDParser()

  val profs = csvReader.parse("data/profs.csv", l => Professor(l(0), l(1)))
  val students = csvReader.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val departments = csvReader.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))

  override def afterAll(): Unit = {
    csvReader.close()
  }

  test("Spark: number of professors") {
    assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) === 3)
  }

  test("Spark: number of students") {
    assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) === 7)
  }

  test("Spark: number of departments") {
    assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) === 3)
  }

  test("Spark: set of students born in 1990") {
    assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""",
      HList("students" -> students)) === Set("Student1", "Student2"))
  }

//  test("number of students born in 1992") {
//    assert(Raw.query( """for (d <- students; d.birthYear = 1992) yield sum 1""", HList("students" -> students)) === 2)
//  }
//
//  test("number of students born before 1991 (included)") {
//    assert(Raw.query( """for (d <- students; d.birthYear <= 1991) yield sum 1""", HList("students" -> students)) === 5)
//  }
//
//  test("set of students in BC123") {
//    assert(Raw.query( """for (d <- students; d.office = "BC123") yield set d.name""", HList("students" -> students)) === Set("Student1", "Student3", "Student5"))
//  }
//
//  test("set of students in dep2") {
//    assert(Raw.query( """for (d <- students; d.department = "dep2") yield set d.name""", HList("students" -> students)) === Set("Student2", "Student4"))
//  }
//
//  test("number of students in dep1") {
//    assert(Raw.query( """for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) === 3)
//  }
//
//  test("set of department (using only students table)") {
//    assert(Raw.query( """for (s <- students) yield set s.department""", HList("students" -> students)) === Set("dep1", "dep2", "dep3"))
//  }
//
//  test("set of department and the headcount (using only students table)") {
//    val r = Raw.query( """
//        for (d <- (for (s <- students) yield set s.department))
//          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
//      HList("students" -> students))
//    println(r)
//    val mr = r.map(_.toMap)
//
//    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
//  }



  // TODO: add these tests to ScalaFlatCSVTest.
  test("Spark: professors as set") {
    val actual = Raw.query("for (d <- profs) yield set d", HList("profs" -> profs)).asInstanceOf[Set[Professor]]
    logger.info("Result: " + actual)
    assert(actual === ReferenceTestData.profs.toSet)
  }

  test("Spark: professors as list") {
    val actual = Raw.query("for (d <- profs) yield list d", HList("profs" -> profs)).asInstanceOf[List[Professor]]
    logger.info("Result: " + actual)
    assert(actual.sorted === ReferenceTestData.profs.sorted)
  }
}
