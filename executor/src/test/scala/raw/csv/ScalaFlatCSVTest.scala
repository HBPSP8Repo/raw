package raw.csv

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite
import raw.{RawQuery, rawQueryAnnotation}

import scala.language.existentials


//case class P(name:String)
//
//class T1 {
//  def go = {
//    val p = P("joe")
//  }
//}


/* Cannot define the object with the query nested inside a class, the macro expansion will fail to typecheck.
 * The typechecker will try to resolve the parent field, which is a reference to the outer class. But this cannot
 * be typechecked until the macro finishes expanding, which causes an exception:
 * "TypecheckException: illegal cyclic reference involving class ..."
 * Probably related to this error: http://grokbase.com/t/gg/scala-user/1511yec3h3/macros-scala-2-10-cannot-typecheck-base-classs-type-params-from-macro-annotation
 */
@rawQueryAnnotation
class Test1(val profs:List[Professor], val students:List[Student]) extends RawQuery {
  val query = "for (d <- profs) yield sum 1"
}

@rawQueryAnnotation
class SetOfDepartmentUsingOnlyStudentsTable(val students:List[Student]) extends RawQuery{
  val query = """for (s <- students) yield set s.department"""
}

@rawQueryAnnotation
class CompositeResult(val students:List[Student]) extends RawQuery {
  val query = """for (d <- students) yield set (name := d.department, number := for (s <- students; s.department = d.department) yield sum 1)"""
}

class ScalaFlatCSVTest extends FunSuite with LazyLogging {
  //  val students = ReferenceTestData.students
  //  val profs = ReferenceTestData.profs
  //  val departments = ReferenceTestData.departments

  //  test("bag of student birth years") {
  //    // Returns an ImmutableMultiset from Guava.
  //    val actual = Raw.query( """for (d <- students) yield bag d.birthYear""", HList("students" -> students))
  //    val expected = ImmutableMultiset.of(1990, 1990, 1989, 1992, 1987, 1992, 1988)
  //    assert(actual === expected)
  //  }

  test("number of professors") {
    assert(new Test1(ReferenceTestData.profs, ReferenceTestData.students).computeResult === 3)
  }

  //  test("number of students") {
  //    assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) === 7)
  //  }
  //
  //  test("number of departments") {
  //    assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) === 3)
  //  }
  //
  //  test("set of students born in 1990") {
  //    assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""", HList("students" -> students)) === Set("Student1", "Student2"))
  //  }
  //
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
  //
  //  test("number of students in dep1") {
  //    assert(Raw.query( """for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) === 3)
  //  }
  //
//
  test("set of department (using only students table)") {
    assert(new SetOfDepartmentUsingOnlyStudentsTable(ReferenceTestData.students).computeResult === Set("dep1", "dep2", "dep3"))
  }




  //
  //  test("set of department and the headcount (using only students table)") {
  //    val r = Raw.query( """
  //        for (d <- (for (s <- students) yield set s.department))
  //          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
  //      HList("students" -> students))
  //
  //    println("Result: " + r)
  //    assert(r.size === 3)
  //
  //    val mr = r.map { case v => Map("name" -> v.name, "count" -> v.count) }
  //    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  //  }
  //
  //  test("set of department and the headcount (using both departments and students table)") {
  //    val r = Raw.query("""
  //        for (d <- (for (d <- departments) yield set d.name))
  //            yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
  //      HList("departments" -> departments, "students" -> students))
  //
  //    assert(r.size === 3)
  //
  //    val mr = r.map { case v => Map("name" -> v.name, "count" -> v.count) }
  //    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  //  }
  //
  //  test("most studied discipline") {
  //    val r = Raw.query("""
  //        for (t <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number =
  //        (for (x <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1))) yield max x.number)) yield set t.name""",
  //      HList("departments" -> departments, "students" -> students))
  //
  //    assert(r.size === 1)
  //    assert(r === Set("Computer Architecture"))
  //  }
  //
  //  test("list of disciplines which have three students") {
  //    val r = Raw.query(
  //      """for (t <- for (d <- departments) yield list (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number = 3) yield list t.name""",
  //      HList("departments" -> departments, "students" -> students))
  //
  //    assert(r.size === 1)
  //    assert(r === List("Computer Architecture"))
  //  }
  //
  test("set of the number of students per department") {
    val r = new CompositeResult(ReferenceTestData.students).computeResult

    assert(r.size === 3)
    val mr = r.map { case v => Map("name" -> v.name, "number" -> v.number) }
    assert(mr === Set(Map("name" -> "dep1", "number" -> 3), Map("name" -> "dep2", "number" -> 2), Map("name" -> "dep3", "number" -> 2)))
  }




  //  test("set of names departments which have the highest number of students", ???, Set("dep1"))
  //  test("set of names departments which have the lowest number of students", ???, Set("dep2", "dep3"))
  //  test("set of profs which have the highest number of students in their department", ???, Set("Prof1"))
  //  test("set of profs which have the lowest number of students in their department", ???, Set("Prof2", "Prof3"))
  //  test("set of students who study 'Artificial Intelligence'", ???, Set("Student6", "Student7"))
}


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
//  check("Bruce Willis movies",s """for (m <- movies; a <- m.actors; a = "Bruce Willis") yield set m.title""", Set("Twelve Monkeys", "Die Hard"))
//  check("Brad Pitt movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt") yield set m.title""", Set("Seven", "Twelve Monkeys"))
//  check("movies with actors born after 1960 (only Brad Pitt)", "for (m <- movies; a <- actors; ma <- m.actors; a.name = ma; a.born > 1960) yield set m.title", Set("Seven", "Twelve Monkeys"))
//  check("Brad Pitt or Bruce Willis movies", """for (m <- movies; a <- m.actors; a = "Brad Pitt" or a = "Bruce Willis") yield set m.title""", Set("Seven", "Twelve Monkeys", "Die Hard"), "#29")
//
//}