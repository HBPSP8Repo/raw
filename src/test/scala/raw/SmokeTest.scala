package raw

//import org.scalatest._
import org.scalatest.FunSuite
import com.typesafe.scalalogging.LazyLogging
import shapeless.HList
import raw.util.{CSVParser, JSONParser}

//abstract class SmokeTest extends FeatureSpec with GivenWhenThen with Matchers {
//  val world: World
//
//  def check(description: String, query: String, expectedResult: Any, issue: String=""): Unit = {
//    val run = (d: String) => if (issue != "") ignore(d + s" ($issue)") _ else scenario(d) _
//    run(description) {
//      When("evaluating '" + query +"'")
//      val result = Query(query, world, executor=executor)
//      Then("it should return " + expectedResult)
//      result match {
//        case Left(v) => println(v) ; assert(false)
//        case Right(q) => assert(q.value === expectedResult)
//      }
//    }
//  }
//}

class FlatCSVTest extends FunSuite with LazyLogging {

  case class Student(name: String, birthYear: Int, office: String, department: String)
  case class Professor(name: String, office: String)
  case class Department(name: String, discipline: String, prof: String)

  val students = CSVParser(
    "test-data/students.csv",
     l => Student(l(0), l(1).toInt, l(2), l(3)))

  val profs = CSVParser(
    "test-data/profs.csv",
    l => Professor(l(0), l(1)))

  val departments = CSVParser(
    "test-data/departments.csv",
    l => Department(l(0), l(1), l(2)))

  test("number of professors") {
    assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) == 3)
  }

  test("number of students") {
    assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) == 7)
  }

  test("number of departments") {
    assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) == 3)
  }

  test("set of students born in 1990") {
    assert(Raw.query("""for (d <- students; d.birthYear = 1990) yield set d.name""", HList("students" -> students)) == Set("Student1", "Student2"))
  }

  test("number of students born in 1992") {
    assert(Raw.query("""for (d <- students; d.birthYear = 1992) yield sum 1""", HList("students" -> students)) == 2)
  }

  test("number of students born before 1991 (included)") {
    assert(Raw.query("""for (d <- students; d.birthYear <= 1991) yield sum 1""", HList("students" -> students)) == 5)
  }

  test("set of students in BC123") {
    assert(Raw.query("""for (d <- students; d.office = "BC123") yield set d.name""", HList("students" -> students)) == Set("Student1", "Student3", "Student5"))
  }

  test("set of students in dep2") {
    assert(Raw.query("""for (d <- students; d.department = "dep2") yield set d.name""", HList("students" -> students)) == Set("Student2", "Student4"))
  }

  test("number of students in dep1") {
    assert(Raw.query("""for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) == 3)
  }

  test("set of department (using only students table)") {
    assert(Raw.query("""for (s <- students) yield set s.department""", HList("students" -> students)) == Set("dep1", "dep2", "dep3"))
  }

  test("set of department and the headcount (using only students table)") {
    val r = Raw.query("""
        for (d <- (for (s <- students) yield set s.department))
          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
      HList("students" -> students))
    println(r)
    val mr = r.map(_.toMap)

    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
//    println(r)
//    r.map(v => println(v.toMap))
    //assert(r)
//    assert(
//      Raw.query("""
//        for (d <- (for (s <- students) yield set s.department))
//          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
//      HList("students" -> students)) == Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  }

//
//  // hard stuff
//  check(,
//  check("set of department and the headcount (using both departments and students table)",
//    """
//        for (d <- (for (d <- departments) yield set d.name))
//             yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1)
//            )""",
//    Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
//
//
//  check("most studied discipline",
//    """for (t <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number =
//      (for (x <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1))) yield max x.number)) yield set t.name
//    """, Set("Computer Architecture"))
//
//  check("list of disciplines which have three students",
//    """for (t <- for (d <- departments) yield list (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number = 3) yield list t.name
//    """, List("Computer Architecture"))
//
//  //check("set of the number of students per department", """for (d <- students) yield set (name := d.department, number := for (s <- students, s.department = d.department) yield sum 1)""",
//  //      Set(Map("name" -> "dep1", "number" -> 3), Map("name" -> "dep2", "number" -> 2), Map("name" -> "dep3", "number" -> 2)))
//  //check("set of names departments which have the highest number of students", ???, Set("dep1"))
//  //check("set of names departments which have the lowest number of students", ???, Set("dep2", "dep3"))
//  //check("set of profs which have the highest number of students in their department", ???, Set("Prof1"))
//  //check("set of profs which have the lowest number of students in their department", ???, Set("Prof2", "Prof3"))
//  //check("set of students who study 'Artificial Intelligence'", ???, Set("Student6", "Student7"))
}

///*
//class FlatCSVSparkTest extends FlatCSVTest {
//  val executor = SparkExecutor
//}
//*/
//
//abstract class HierarchyJSONTest extends SmokeTest {
//  val movies = JSONParser(
//    ListType(RecordType(List(AttrType("title", StringType()), AttrType("year", IntType()), AttrType("actors", ListType(StringType()))))),
//    "src/test/data/smokeTest/movies.json")
//
//  val actors = JSONParser(
//    ListType(RecordType(List(AttrType("name", StringType()), AttrType("born", IntType())))),
//    "src/test/data/smokeTest/actors.json")
//
//  val world = new World(dataSources=Map(
//    "movies" -> ScalaDataSource(movies),
//    "actors" -> ScalaDataSource(actors)
//  ))
//
//  // some sanity check
//  check("number of movies", "for (m <- movies) yield sum 1", 3)
//  check("number of actors", "for (a <- actors) yield sum 1", 4)
//
//  // simple stuff
//  check("most recent movie release year", "for (y <- (for (m <- movies) yield set m.year)) yield max y", 1995)
//  check("set of most recent movies", "for (m <- movies; m.year = (for (y <- (for (m <- movies) yield set m.year)) yield max y)) yield set m.title", Set("Twelve Monkeys", "Seven"))
//  check("set of actors in 'Seven'", "for (m <- movies; m.title = \"Seven\"; a <- m.actors) yield set a", Set("Brad Pitt", "Morgan Freeman", "Kevin Spacey"))
//
//  // harder
//  check("Bruce Willis movies", """for (m <- movies; a <- m.actors; a = "Bruce Willis") yield set m.title""", Set("Twelve Monkeys", "Die Hard"))
//  check("Brad Pitt movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt") yield set m.title""", Set("Seven", "Twelve Monkeys"))
//  check("movies with actors born after 1960 (only Brad Pitt)", "for (m <- movies; a <- actors; ma <- m.actors; a.name = ma; a.born > 1960) yield set m.title", Set("Seven", "Twelve Monkeys"))
//  check("Brad Pitt or Bruce Willis movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt" or a = "Bruce Willis") yield set m.title""", Set("Seven", "Twelve Monkeys", "Die Hard"), "#29")
//
//}
//
//class HierarchyJSONReferenceTest extends HierarchyJSONTest {
//  val executor = ReferenceExecutor
//}
//
///*
//class HierarchyJSONSparkTest extends HierarchyJSONTest {
//  val executor = SparkExecutor
//}
//*/