package raw.csv

import com.google.common.collect.ImmutableMultiset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}
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

//  test("bag of student birth years") {
//    // Returns an ImmutableMultiset from Guava.
//    val actual = Raw.query( """for (d <- students) yield bag d.birthYear""", HList("students" -> students))
//    println("res: " + actual + " " + actual.getClass)

//    val b = ImmutableMultiset.builder[Any]()
//    actual.foreach( {case (value, count)  => b.addCopies(value, count.toInt)} )
//    val res = b.build()
//    println("Result: " + res)
//    val expected = ImmutableMultiset.of(1990, 1990, 1989, 1992, 1987, 1992, 1988)
//    assert(actual === expected)
//  }


  //  ignore("spark join") {
  //    println("Students:\n" + students.collect().mkString("\n"))
  //    println("Profs:\n" + profs.collect.mkString("\n"))
  //    val res: RDD[(Student, Professor)] = students.cartesian(profs)
  //    println(s"Students x Profs:\n${res.collect.mkString("\n")}")
  //    val filtered = res.filter({ case (s, p) => s.name.last == p.name.last })
  //    println(s"Students x Profs:\n${filtered.collect.mkString("\n")}")
  //  }

  //  test("Spark: cross product professors x departments x departments") {
  //    // How can we cast to a Set[X] where X is a class known by the client API?
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)",
  //      HList("profs" -> profs, "students" -> students, "departments" -> departments)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //    assert(q.size === 3*3*7)
  //  }
  //
  //  test("Spark: cross product professors x departments") {
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments) yield set (p.name, p.office, d.name, d.discipline, d.prof)",
  //      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //    assert(q.size === 9)
  //  }
  //
  //  test("Spark: inner join professors x departments") {
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments; p.name = d.prof) " +
  //        "yield set (name := p.name, officeName := p.office, deptName := d.name, discipline := d.discipline)",
  //      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //    assert(q.size === 3)
  //  }
  //
  //  test("Spark JOIN: (professors, departments) with predicate") {
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments; d.name.last = p.name.last) " +
  //        "yield set (p.name, p.office, d.name, d.discipline, d.prof)",
  //      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //    assert(q.size === 3)
  //  }

  //  test("Spark JOIN: (professors, departments, students) with one predicate per datasource") {
  //    val q: Set[Any] = Raw.query(
  //      """for (p <- profs; d <- departments; s <- students;
  //              p.name <> "Dr, Who"; d.name <> "Magic University"; s.name <> "Batman")
  //        yield set (p.name, p.office, d.name, d.discipline, d.prof, s.name)""",
  //      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //  }

  //  test("Spark JOIN: (professors, departments, students) predicate with triple condition") {
  //    val q: Set[Any] = Raw.query(
  //      "for (p <- profs; d <- departments; s <- students; " +
  //        "d.name <> p.name and p.name <> s.name and s.name <> d.name) " +
  //        "yield set (p.name, d.name, s.name)",
  //      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
  //    printQueryResult(q)
  //  }

  def printQueryResult(res: Set[Any]) = {
    val str = res.map(r => r.asInstanceOf[ {def toMap(): Map[Any, Any]}].toMap()).mkString("\n")
    println("Result:\n" + str)
  }

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

  /* ======================================
   Primitive monoids
    ====================================== */
//  test("Spark: [sum] of the students birth year") {
//    assert(Raw.query("for (s <- students) yield sum s.birthYear", HList("students" -> students)) === 13928)
//  }
  //
  //  test("Spark: [max] of the students birth year") {
  //    assert(Raw.query("for (s <- students) yield max s.birthYear", HList("students" -> students)) === 1992)
  //  }
  //
  //  test("Spark: [multiply] product of the students birth year") {
  //    assert(Raw.query("for (s <- students; s.birthYear < 1989) yield multiply s.birthYear", HList("students" -> students)) === 3950156)
  //  }
  //
  //  test("Spark: [and] not all students were born before 1990") {
  //    assert(Raw.query("for (s <- students) yield and s.birthYear<1990", HList("students" -> students)) === false)
  //  }
  //
  //  test("Spark: [and] all students were born after 1980") {
  //    assert(Raw.query("for (s <- students) yield and s.birthYear>1980", HList("students" -> students)) === true)
  //  }
  //
  //  test("Spark: [or] at least one student was born in 1990") {
  //    assert(Raw.query("for (s <- students) yield or s.birthYear=1990", HList("students" -> students)) === true)
  //  }
  //
  //  test("Spark: [or] no student was born in 1991") {
  //    assert(Raw.query("for (s <- students) yield or s.birthYear=1991", HList("students" -> students)) === false)
  //  }

  /* ======================================
   Merge monoids: sum, max, multiply, and, or
    ====================================== */
  //  test("Spark: [merge and] Some students born between 1990 and 1995, exclusive") {
  //    assert(Raw.query("for (s <- students) yield or (s.birthYear>1990 and s.birthYear<1995)", HList("students" -> students)) === true)
  //  }
  // TODO: Other merge monoids not yet implemented in the executor

  /* ======================================
  Collection monoids: List, Set, Bag
  ====================================== */
  test("Spark: [set monoid] All student names") {
    val actual = Raw.query("for (s <- students) yield set s.birthYear", HList("students" -> students))
    val expected = Set(1990, 1989, 1987, 1992, 1988)
    assert(actual === expected)
  }
//
//  test("Spark: [list monoid] All student names") {
//    val actual = Raw.query("for (s <- students) yield list s.birthYear", HList("students" -> students)).asInstanceOf[List[Int]]
//    val expected = List(1990, 1990, 1989, 1992, 1987, 1992, 1988)
//    assert(actual.sorted === expected.sorted)
//  }

  //  test("Spark: set students names") {
  //    assert(Raw.query("for (s <- students) yield set s.name", HList("students" -> students)) === 13928)
  //  }
  //
  //  test("Spark: max of the students birth year") {
  //    assert(Raw.query("for (s <- students) yield max s.birthYear", HList("students" -> students)) === 13928)
  //  }
  //
  //  test("Spark: product of the students birth year") {
  //    assert(Raw.query("for (s <- students) yield multiply s.birthYear", HList("students" -> students)) === 13928)
  //  }


  //    test("Spark: number of professors") {
  //      assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) === 3)
  //    }
  //
  //    test("Spark: number of students") {
  //      assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) === 7)
  //    }
  //
  //    test("Spark: number of departments") {
  //      assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) === 3)
  //    }
  //
  //    test("Spark: set of students born in 1990") {
  //      assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""",
  //        HList("students" -> students)) === Set("Student1", "Student2"))
  //    }
  //
  //    test("number of students born in 1992") {
  //      assert(Raw.query( """for (d <- students; d.birthYear = 1992) yield sum 1""", HList("students" -> students)) === 2)
  //    }
  //
  //    test("number of students born before 1991 (included)") {
  //      assert(Raw.query( """for (d <- students; d.birthYear <= 1991) yield sum 1""", HList("students" -> students)) === 5)
  //    }
  //
  //    test("set of students in BC123") {
  //      assert(Raw.query( """for (d <- students; d.office = "BC123") yield set d.name""", HList("students" -> students)) === Set("Student1", "Student3", "Student5"))
  //    }
  //
  //    test("set of students in dep2") {
  //      assert(Raw.query( """for (d <- students; d.department = "dep2") yield set d.name""", HList("students" -> students)) === Set("Student2", "Student4"))
  //    }
  //
  //    test("number of students in dep1") {
  //      assert(Raw.query( """for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) === 3)
  //    }
  //
  //    test("set of department (using only students table)") {
  //      assert(Raw.query( """for (s <- students) yield set s.department""", HList("students" -> students)) === Set("dep1", "dep2", "dep3"))
  //    }

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
