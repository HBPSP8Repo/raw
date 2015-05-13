package raw.csv.spark

import org.apache.spark.rdd.RDD
import raw.csv._
import raw.{RawQuery, rawQueryAnnotation}

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
@rawQueryAnnotation
class Cross_product_professors_x_departments_x_students(students: RDD[Student], departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query: String = """for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)"""
}

@rawQueryAnnotation
class Cross_product_professors_x_departments(departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query: String = """for (p <- profs; d <- departments) yield set (p.name, p.office, d.name, d.discipline, d.prof)"""
}

@rawQueryAnnotation
class Inner_join_professors_x_departments(departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query = """ for (p <- profs; d <- departments; p.name = d.prof)  yield set (name := p.name, officeName := p.office, deptName := d.name, discipline := d.discipline) """
}

@rawQueryAnnotation
class Professors_x_departments_with_predicate(departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query = """ for (p <- profs; d <- departments; d.name.last = p.name.last) yield set (p.name, p.office, d.name, d.discipline, d.prof) """
}

@rawQueryAnnotation
class Professors_departments_students_with_one_predicate_per_datasource(students: RDD[Student], departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query = """ for (p <- profs; d <- departments; s <- students;
                  p.name <> "Dr, Who"; d.name <> "Magic University"; s.name <> "Batman")
            yield set (p.name, p.office, d.name, d.discipline, d.prof, s.name)"""
}

@rawQueryAnnotation
class Professors_departments_students_predicate_with_triple_condition(students: RDD[Student], departments: RDD[Department], profs: RDD[Professor]) extends RawQuery {
  val query = """ for (p <- profs; d <- departments; s <- students;
        d.name <> p.name and p.name <> s.name and s.name <> d.name)
        yield set (p.name, d.name, s.name) """
}

@rawQueryAnnotation
class Set_of_department_and_the_headcount_using_only_students_table(students: RDD[Student]) extends RawQuery {
  val query = """for (d <- (for (s <- students) yield set s.department))
            yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))"""
}


class SparkFlatCSVJoinTest extends AbstractSparkFlatCSVTest {
  test("cross product professors x departments x students") {
    val res = Cross_product_professors_x_departments_x_students(testData.students, testData.departments, testData.profs)
    assert(res.size === 3 * 3 * 7)
  }

  test("cross product professors x departments") {
    val res = Cross_product_professors_x_departments(testData.departments, testData.profs).asInstanceOf[Set[Any]]
    assert(res.size === 9)
  }

  test("inner join professors x departments") {
    val res = new Inner_join_professors_x_departments(testData.departments, testData.profs).computeResult.asInstanceOf[Set[Any]]
    assert(res.size === 3)
  }

  test("(professors, departments) with predicate") {
    val res = new Professors_x_departments_with_predicate(testData.departments, testData.profs).computeResult.asInstanceOf[Set[Any]]
    assert(res.size === 3)
  }


  test("Spark JOIN: (professors, departments, students) with one predicate per datasource") {
    val q = new Professors_departments_students_with_one_predicate_per_datasource(testData.students, testData.departments, testData.profs).computeResult.asInstanceOf[Set[Any]]
    printQueryResult(q)
  }

  test("Spark JOIN: (professors, departments, students) predicate with triple condition") {
    val q: Set[Any] = new Professors_departments_students_predicate_with_triple_condition(testData.students, testData.departments, testData.profs).computeResult.asInstanceOf[Set[Any]]
    printQueryResult(q)
  }

  test("Set Of Department and headcount using only students table") {
    val r = new Set_of_department_and_the_headcount_using_only_students_table(testData.students).computeResult
    printQueryResult(r)
    assert(r.size === 3)
    val mr = r.map { case v => Map("name" -> v.name, "count" -> v.count) }
    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  }


  def printQueryResult(res: Set[_]) = {
    val str = res.mkString("\n")
    println("Result:\n" + str)
  }
}
