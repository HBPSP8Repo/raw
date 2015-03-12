package raw

import org.scalatest._
import executor.Executor
import executor.reference.ReferenceExecutor

abstract class SmokeTest extends FeatureSpec with GivenWhenThen with Matchers {
  val world: World
  val executor: Executor

  def check(description: String, query: String, expectedResult: Any, issue: String=""): Unit = {
    val run = (d: String) => if (issue != "") ignore(d + s" ($issue)") _ else scenario(d) _
    run(description) {
      When("evaluating '" + query +"'")
      val result = Query(query, world, executor=executor)
      Then("it should return " + expectedResult)
      result match {
        case Left(v) => println(v) ; assert(false)
        case Right(q) => assert(q.value === expectedResult)
      }
    }
  }
}

abstract class FlatCSVTest extends SmokeTest {

  val students = LocalFileLocation(
    ListType(RecordType(List(AttrType("name",StringType()), AttrType("birthYear", IntType()), AttrType("office", StringType()), AttrType("department", StringType())))),
    "src/test/data/smokeTest/students.csv",
    TextCsv(","))

  val profs = LocalFileLocation(
    ListType(RecordType(List(AttrType("name",StringType()), AttrType("office", StringType())))),
    "src/test/data/smokeTest/profs.csv",
    TextCsv(","))

  val departments = LocalFileLocation(
    ListType(RecordType(List(AttrType("name",StringType()), AttrType("discipline", StringType()), AttrType("prof", StringType())))),
    "src/test/data/smokeTest/departments.csv",
    TextCsv(","))

  val world: World = new World(Map(
    "students" -> students,
    "profs" -> profs,
    "departments" -> departments))

  // some sanity check on the file content basically
  check("number of professors", "for (d <- profs) yield sum 1", 3)
  check("number of students", "for (d <- students) yield sum 1", 7)
  check("number of departments", "for (d <- departments) yield sum 1", 3)

  // more fancy stuff
  check("set of students born in 1990", """for (d <- students; d.birthYear = 1990) yield set d.name""", Set("Student1", "Student2"))
  check("number of students born in 1992", """for (d <- students; d.birthYear = 1992) yield sum 1""", 2)
  check("number of students born before 1991 (included)", """for (d <- students; d.birthYear <= 1991) yield sum 1""", 5)
  check("set of students in BC123", """for (d <- students; d.office = "BC123") yield set d.name""", Set("Student1", "Student3", "Student5"))
  check("set of students in dep2", """for (d <- students; d.department = "dep2") yield set d.name""", Set("Student2", "Student4"))
  check("number of students in dep1", """for (d <- students; d.department = "dep1") yield sum 1""", 3)
  check("set of department (using only students table)", """for (s <- students) yield set s.department""", Set("dep1", "dep2", "dep3"))

  // hard stuff
  check("set of department and the headcount (using only students table)",
    """
        for (d <- (for (s <- students) yield set s.department))
             yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1)
            )""",
    Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  check("set of department and the headcount (using both departments and students table)",
    """
        for (d <- (for (d <- departments) yield set d.name))
             yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1)
            )""",
    Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))


  check("most studied discipline",
    """for (t <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number =
      (for (x <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1))) yield max x.number)) yield set t.name
    """, Set("Computer Architecture"))

  check("list of disciplines which have three students",
    """for (t <- for (d <- departments) yield list (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number = 3) yield list t.name
    """, List("Computer Architecture"))

  //check("set of the number of students per department", """for (d <- students) yield set (name := d.department, number := for (s <- students, s.department = d.department) yield sum 1)""",
  //      Set(Map("name" -> "dep1", "number" -> 3), Map("name" -> "dep2", "number" -> 2), Map("name" -> "dep3", "number" -> 2)))
  //check("set of names departments which have the highest number of students", ???, Set("dep1"))
  //check("set of names departments which have the lowest number of students", ???, Set("dep2", "dep3"))
  //check("set of profs which have the highest number of students in their department", ???, Set("Prof1"))
  //check("set of profs which have the lowest number of students in their department", ???, Set("Prof2", "Prof3"))
  //check("set of students who study 'Artificial Intelligence'", ???, Set("Student6", "Student7"))
}

class FlatCSVReferenceTest extends FlatCSVTest {
  val executor = ReferenceExecutor
}

/*
class FlatCSVSparkTest extends FlatCSVTest {
  val executor = SparkExecutor
}
*/

abstract class HierarchyJSONTest extends SmokeTest {
  val movies = LocalFileLocation(
    ListType(RecordType(List(AttrType("title", StringType()), AttrType("year", IntType()), AttrType("actors", SetType(StringType()))))),
    "src/test/data/smokeTest/movies.json",
    ApplicationJson)

  val actors = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("born", IntType())))),
    "src/test/data/smokeTest/actors.json",
    ApplicationJson)

  val world = new World(Map(
    "movies" -> movies,
    "actors" -> actors
  ))

  // some sanity check
  check("number of movies", "for (m <- movies) yield sum 1", 3)
  check("number of actors", "for (a <- actors) yield sum 1", 4)

  // simple stuff
  check("most recent movie release year", "for (y <- (for (m <- movies) yield set m.year)) yield max y", 1995)
  check("set of most recent movies", "for (m <- movies; m.year = (for (y <- (for (m <- movies) yield set m.year)) yield max y)) yield set m.title", Set("Twelve Monkeys", "Seven"))
  check("set of actors in 'Seven'", "for (m <- movies; m.title = \"Seven\"; a <- m.actors) yield set a", Set("Brad Pitt", "Morgan Freeman", "Kevin Spacey"))

  // harder
  check("Bruce Willis movies", """for (m <- movies; a <- m.actors; a = "Bruce Willis") yield set m.title""", Set("Twelve Monkeys", "Die Hard"))
  check("Brad Pitt movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt") yield set m.title""", Set("Seven", "Twelve Monkeys"))
  check("movies with actors born after 1960 (only Brad Pitt)", "for (m <- movies; a <- actors; ma <- m.actors; a.name = ma; a.born > 1960) yield set m.title", Set("Seven", "Twelve Monkeys"))
  check("Brad Pitt or Bruce Willis movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt" or a = "Bruce Willis") yield set m.title""", Set("Seven", "Twelve Monkeys", "Die Hard"), "#29")

}



class HierarchyJSONReferenceTest extends HierarchyJSONTest {
  val executor = ReferenceExecutor
}

/*
class HierarchyJSONSparkTest extends HierarchyJSONTest {
  val executor = SparkExecutor
}
*/