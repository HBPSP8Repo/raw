package raw.csv

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.Raw
import raw.util.CSVToRDDParser
import shapeless.HList

import scala.language.reflectiveCalls

class SparkFlatCSVTest extends FunSuite with LazyLogging with BeforeAndAfterAll {
  val csvReader = new CSVToRDDParser()

  val profs = csvReader.parse("data/profs.csv", l => Professor(l(0), l(1)))
  val students = csvReader.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val departments = csvReader.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))

  override def afterAll(): Unit = {
    csvReader.close()
  }

//  ignore("spark join") {
//    println("Students:\n" + students.collect().mkString("\n"))
//    println("Profs:\n" + profs.collect.mkString("\n"))
//    val res: RDD[(Student, Professor)] = students.cartesian(profs)
//    println(s"Students x Profs:\n${res.collect.mkString("\n")}")
//    val filtered = res.filter({ case (s, p) => s.name.last == p.name.last })
//    println(s"Students x Profs:\n${filtered.collect.mkString("\n")}")
//  }

  test("Spark: cross product professors x departments x departments") {
    // How can we cast to a Set[X] where X is a class known by the client API?
    val q: Set[Any] = Raw.query(
      "for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)",
      HList("profs" -> profs, "students" -> students, "departments" -> departments)).asInstanceOf[Set[Any]]
    printQueryResult(q)
    assert(q.size === 3*3*7)
  }

  test("Spark: cross product professors x departments") {
    val q: Set[Any] = Raw.query(
      "for (p <- profs; d <- departments) yield set (p.name, p.office, d.name, d.discipline, d.prof)",
      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
    printQueryResult(q)
    assert(q.size === 9)
  }

  test("Spark: inner join professors x departments") {
    val q: Set[Any] = Raw.query(
      "for (p <- profs; d <- departments; p.name = d.prof) " +
        "yield set (name := p.name, officeName := p.office, deptName := d.name, discipline := d.discipline)",
      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
    printQueryResult(q)
    assert(q.size === 3)
  }

  def printQueryResult(res: Set[Any]) = {
    val str = res.map(r => r.asInstanceOf[{def toMap(): Map[Any, Any]}].toMap()).mkString("\n")
    println("Result:\n"+str)
  }

  //  test("Spark JOIN: professors and departments with filter") {
  //    // How can we cast to a Set[X] where X is a class known by the client API?
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments; d.name.last = p.name.last) yield set (p.name, p.office, d.name, d.discipline, d.prof)",
  //      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
  //    println(q)
  //    assert(q.size === 3)
  //  }

  //  test("spark query") {
  //    profs
  //      .filter(((arg) => true))
  //      .cartesian(departments.filter(((arg) => true)))
  //      .filter(((arg) => true))
  //      .filter(((arg) => true))
  //      .map(((arg) => {
  //      final class $anon extends scala.AnyRef with Serializable {
  //        def toMap = Map("_1".$minus$greater(_1), "_2".$minus$greater(_2), "_3".$minus$greater(_3), "_4".$minus$greater(_4), "_5".$minus$greater(_5));
  //        val _1 = arg._1.name;
  //        val _2 = arg._1.office;
  //        val _3 = arg._2.name;
  //        val _4 = arg._2.discipline;
  //        val _5 = arg._2.prof
  //      };
  //      new $anon()
  //    })).toLocalIterator.toSet
  //  }


  //  test("Spark: number of professors") {
  //    assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) === 3)
  //  }
  //
  //  test("Spark: number of students") {
  //    assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) === 7)
  //  }
  //
  //  test("Spark: number of departments") {
  //    assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) === 3)
  //  }
  //
  //  test("Spark: set of students born in 1990") {
  //    assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""",
  //      HList("students" -> students)) === Set("Student1", "Student2"))
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
  //  test("number of students in dep1") {
  //    assert(Raw.query( """for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) === 3)
  //  }
  //
  //  test("set of department (using only students table)") {
  //    assert(Raw.query( """for (s <- students) yield set s.department""", HList("students" -> students)) === Set("dep1", "dep2", "dep3"))
  //  }

  //  test("spark") {
  //    ${build(left)}.flatMap(l =>
  //      if (l == null)
  //        List((null, null))
  //      else {
  //        val ok = ${build(right)}.map(r => (l, r)).filter(${exp(p)})
  //        if (ok.isEmpty)
  //          List((l, null))
  //        else
  //          ok.map(r => (l, r))
  //      }
  //    )
  //    """
  //    val r: RDD[Student] = students.filter(((arg) => true))
  ////    val res = r.flatMap { case l => List((None, (None, None)))}
  //    val res: RDD[(Student, (Student, Student))] = r.flatMap { case l =>
  //      if (l == null) {
  //        val res = List((null, (null, null)))
  //        res
  //      } else {
  //        val ok = students.map { case r => (l, r) }
  //        if (ok.isEmpty()) {
  //          val res = List(scala.Tuple2(l, (null, null)))
  //          res
  //        } else {
  //          //          ???
  //          val res = ok.map { case r => (l, r) }.collect().toList
  //          res
  //        }
  //      }
  //    }
  //  }
  //      flatMap(((l) => if (l.$eq$eq(null))
  //      List(scala.Tuple2(null, null))
  //    else
  //    {
  //      val ok = SparkFlatCSVTest.this.students.filter(((arg) => true)).map(((r) => scala.Tuple2(l, r))).filter(((arg) => true));
  //      if (ok.isEmpty)
  //        List(scala.Tuple2(l, null))
  //      else
  //        ok.map(((r) => scala.Tuple2(l, r)))
  //    }))
  //}

  // Needs Nest
  //    test("set of department and the headcount (using only students table)") {
  //      val r = Raw.query(
  //        //        for (d <- (for (s <- students) yield set s.department))
  //        //          yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
  //        """for (d <- (for (s <- students) yield set s.department))
  //      yield set (name := d, count := (for (s <- students) yield sum 1))""",
  //        //      """for (d <- (for (s <- students) yield set s.department)) yield set (count := (for (s <- students; s.department = d) yield sum 1))""",
  //        HList("students" -> students))
  //      println(r)
  //    }
  //  // Needs Nest
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
  //  test("Spark: professors as set") {
  //    val actual = Raw.query("for (d <- profs) yield set d", HList("profs" -> profs)).asInstanceOf[Set[Professor]]
  //    logger.info("Result: " + actual)
  //    assert(actual === ReferenceTestData.profs.toSet)
  //  }
  //
  //  test("Spark: professors as list") {
  //    val actual = Raw.query("for (d <- profs) yield list d", HList("profs" -> profs)).asInstanceOf[List[Professor]]
  //    logger.info("Result: " + actual)
  //    assert(actual.sorted === ReferenceTestData.profs.sorted)
  //  }
}
