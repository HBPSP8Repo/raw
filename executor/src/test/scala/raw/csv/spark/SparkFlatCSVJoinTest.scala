package raw.csv.spark

import org.apache.spark.rdd.RDD
import raw.csv._
import raw.{RawQuery, rawQueryAnnotation}

@rawQueryAnnotation
class cross_product_professors_x_departments_x_students(students: RDD[Student], departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query: String = """for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)"""
}

/*
 The macro will for some queries generate case classes. If the generated code is inserted into a class, the Scala
 compiler will automatically link it to an instance of the outer class, by adding an hidden reference to the outer class.
 This means that when Spark tries to serialize an instance of the case class, the outer class will be pulled together,
 which is not intended. The typical approach for case classes is to declare them on a companion object, therefore
 avoiding any references to other classes.

 Since the generated code of a macro is always inserted at the point of call, we cannot from the macro generate
 any code outside this scope. This requires that we call the macro from within an object for the queries that generate
 case classes (usually, the ones with joins)
 */
//object SparkFlatCSVJoinTest {
//  var students: RDD[Student] = null
//  var profs: RDD[Professor] = null
//  var departments: RDD[Department] = null
//
//  def init(testData: TestData): Unit = {
//    profs = testData.profs
//    departments = testData.departments
//    students = testData.students
//  }

//  def cross_product_professors_x_departments_x_students(): Set[Any] = {
//    // How can we cast to a Set[X] where X is a class known by the client API?
//    Raw.query(
//      "for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)",
//      HList("profs" -> profs, "students" -> students, "departments" -> departments)).asInstanceOf[Set[Any]]
//  }

//  def cross_product_professors_x_departments(): Set[Any] = {
//    Raw.query("for (p <- profs; d <- departments) yield set (p.name, p.office, d.name, d.discipline, d.prof)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
//  }
//
//  def inner_join_professors_x_departments(): Set[Any] = {
//    Raw.query("for (p <- profs; d <- departments; p.name = d.prof) " +
//      "yield set (name := p.name, officeName := p.office, deptName := d.name, discipline := d.discipline)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
//  }
//
//  def professors_x_departments_with_predicate(): Set[Any] = {
//    Raw.query("for (p <- profs; d <- departments; d.name.last = p.name.last) " +
//      "yield set (p.name, p.office, d.name, d.discipline, d.prof)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
//  }
//
//  def professors_departments_students_with_one_predicate_per_datasource = {
//    Raw.query( """for (p <- profs; d <- departments; s <- students;
//                  p.name <> "Dr, Who"; d.name <> "Magic University"; s.name <> "Batman")
//            yield set (p.name, p.office, d.name, d.discipline, d.prof, s.name)""",
//      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
//  }
//
//  def professors_departments_students_predicate_with_triple_condition = {
//    Raw.query(
//      "for (p <- profs; d <- departments; s <- students; " +
//        "d.name <> p.name and p.name <> s.name and s.name <> d.name) " +
//        "yield set (p.name, d.name, s.name)",
//      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
//  }
//
////  def set_of_department_and_the_headcount_using_only_students_table() = {
////    Raw.query( """
////        for (d <- (for (s <- students) yield set s.department))
////          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
////      HList("students" -> students)).asInstanceOf[Set[Any]]
////  }
//}
//
//
class SparkFlatCSVJoinTest extends AbstractSparkFlatCSVTest {
  //    SparkFlatCSVJoinTest.init(testData)

  test("cross product professors x departments x students") {
    val res = new cross_product_professors_x_departments_x_students(testData.students, testData.departments, testData.profs)
      .computeResult.asInstanceOf[Set[Any]]
    assert(res.size ===   3 * 3 * 7)
  }

  //  test("cross product professors x departments") {
  //    val res = SparkFlatCSVJoinTest.cross_product_professors_x_departments()
  //    assert(res.size === 9)
  //  }
  //
  //  test("inner join professors x departments") {
  //    val res = SparkFlatCSVJoinTest.inner_join_professors_x_departments()
  //    assert(res.size === 3)
  //  }
  //
  //  test("(professors, departments) with predicate") {
  //    val res = SparkFlatCSVJoinTest.professors_x_departments_with_predicate()
  //    assert(res.size === 3)
  //  }
  //
  //  test("Spark JOIN: (professors, departments, students) with one predicate per datasource") {
  //    val q: Set[Any] = SparkFlatCSVJoinTest.professors_departments_students_with_one_predicate_per_datasource
  //    printQueryResult(q)
  //  }
  //
  //  test("Spark JOIN: (professors, departments, students) predicate with triple condition") {
  //    val q: Set[Any] = SparkFlatCSVJoinTest.professors_departments_students_predicate_with_triple_condition
  //    printQueryResult(q)
  //  }
  //
  //  // TODO: Needs nest.
  //  test("set of department and the headcount (using only students table)") {
  //    //    val r = SparkFlatCSVJoinTest.set_of_department_and_the_headcount_using_only_students_table()
  //
  //    //    assert(r.size === 3)
  //    //    val mr = r.map { case v => Map("name" -> v.name, "count" -> v.count) }
  //    //    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  //  }

  def printQueryResult(res: Set[Any]) = {
    val str = res.mkString("\n")
    println("Result:\n" + str)
  }
}
