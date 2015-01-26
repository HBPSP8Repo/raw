package raw

import org.scalatest._
import raw.executor.reference.ReferenceExecutor

/**
 * Created by gaidioz on 1/23/15.
 */
class SmokeTest extends FeatureSpec with GivenWhenThen with Matchers {
  {

    val students = LocalFileLocation("src/test/data/smokeTest/students.csv", "text/csv")
    val profs = LocalFileLocation("src/test/data/smokeTest/profs.csv", "text/csv")
    val departments = LocalFileLocation("src/test/data/smokeTest/departments.csv", "text/csv")
    val world: World = new World(Map(
          "students" -> Source(CollectionType(ListMonoid(),RecordType(List(AttrType("name",StringType()), AttrType("birthYear", IntType()), AttrType("office", StringType()), AttrType("department", StringType())))), students),
          "profs" -> Source(CollectionType(ListMonoid(),RecordType(List(AttrType("name",StringType()), AttrType("office", StringType())))), profs),
          "departments" -> Source(CollectionType(ListMonoid(),RecordType(List(AttrType("name",StringType()), AttrType("discipline", StringType()), AttrType("prof", StringType())))), departments)))

    def check(description: String, query: String, expectedResult: Any): Unit = {
      scenario(description) {
        When("evaluating '" + query +"'")
        val result = Query(query, world, executor=ReferenceExecutor)
        Then("it should return " + expectedResult)
        result match {
          case Left(v) => println(v) ; assert(false)
          case Right(q) => assert(q.value === expectedResult)
        }
      }
    }

    // some sanity check on the file content basically
    check("number of professors", "for (d <- profs) yield sum 1", 3)
    check("number of students", "for (d <- students) yield sum 1", 7)
    check("number of departments", "for (d <- departments) yield sum 1", 3)
    check("set of students born in 1990", """for (d <- students, d.birthYear = 1990) yield set d.name""", Set("Student1", "Student2"))
    check("number of students born in 1992", """for (d <- students, d.birthYear = 1992) yield sum 1""", 2)
    check("number of students born before 1991 (included)", """for (d <- students, d.birthYear <= 1991) yield sum 1""", 5)
    check("set of students in BC123", """for (d <- students, d.office = "BC123") yield set d.name""", Set("Student1", "Student3", "Student5"))
    check("set of students in dep2", """for (d <- students, d.department = "dep2") yield set d.name""", Set("Student2", "Student4"))
    check("number of students in dep1", """for (d <- students, d.department = "dep1") yield sum 1""", 3)
    check("set of department (using only students table)", """for (s <- students) yield set s.department""", Set("dep1", "dep2", "dep3"))
    check("set of department and the headcount (using only students table)",
      """
        for (d <- (for (s <- students) yield set s.department))
             yield set (name := d, count := (for (s <- students, s.department = d) yield sum 1)
            )""",
          Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
    check("set of department and the headcount (using both departments and students table)",
      """
        for (d <- (for (d <- departments) yield set d.name))
             yield set (name := d, count := (for (s <- students, s.department = d) yield sum 1)
            )""",
      Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))

    //check("most studied discipline", """for (d <- department) yield set (name := d.discipline, number := """, "Computer Architecture")
    //check("set of the number of students per department", """for (d <- students) yield set (name := d.department, number := for (s <- students, s.department = d.department) yield sum 1)""",
    //      Set(Map("name" -> "dep1", "number" -> 3), Map("name" -> "dep2", "number" -> 2), Map("name" -> "dep3", "number" -> 2)))
    //check("set of names departments which have the highest number of students", ???, Set("dep1"))
    //check("set of names departments which have the lowest number of students", ???, Set("dep2", "dep3"))
    //check("set of profs which have the highest number of students in their department", ???, Set("Prof1"))
    //check("set of profs which have the lowest number of students in their department", ???, Set("Prof2", "Prof3"))
    //check("set of students who study 'Artificial Intelligence'", ???, Set("Student6", "Student7"))
  }
}
