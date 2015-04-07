package raw.csv

import com.google.common.collect.ImmutableMultiset
import raw.Raw
import shapeless.HList

class SparkFlatCSVBasicTest extends AbstractSparkFlatCSVTest {
  /* ======================================
   Primitive monoids
    ====================================== */
  test("number of professors") {
    assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) === 3)
  }

  test("number of students") {
    assert(Raw.query("for (d <- students) yield sum 1", HList("students" -> students)) === 7)
  }

  test("number of departments") {
    assert(Raw.query("for (d <- departments) yield sum 1", HList("departments" -> departments)) === 3)
  }

  test("[sum] of the students birth year") {
    assert(Raw.query("for (s <- students) yield sum s.birthYear", HList("students" -> students)) === 13928)
  }

  test("[max] of the students birth year") {
    assert(Raw.query("for (s <- students) yield max s.birthYear", HList("students" -> students)) === 1992)
  }

  test("[multiply] product of the students birth year") {
    assert(Raw.query("for (s <- students; s.birthYear < 1989) yield multiply s.birthYear", HList("students" -> students)) === 3950156)
  }

  test("[and] not all students were born before 1990") {
    assert(Raw.query("for (s <- students) yield and s.birthYear<1990", HList("students" -> students)) === false)
  }

  test("[and] all students were born after 1980") {
    assert(Raw.query("for (s <- students) yield and s.birthYear>1980", HList("students" -> students)) === true)
  }

  test("[or] at least one student was born in 1990") {
    assert(Raw.query("for (s <- students) yield or s.birthYear=1990", HList("students" -> students)) === true)
  }

  test("[or] no student was born in 1991") {
    assert(Raw.query("for (s <- students) yield or s.birthYear=1991", HList("students" -> students)) === false)
  }

  /* ======================================
   Merge monoids: sum, max, multiply, and, or
    ====================================== */
  test("[merge and] Some students born between 1990 and 1995, exclusive") {
    assert(Raw.query("for (s <- students) yield or (s.birthYear>1990 and s.birthYear<1995)", HList("students" -> students)) === true)
  }
  // TODO: Other merge monoids not yet implemented in the executor

  /* ======================================
  Collection monoids: List, Set, Bag
  ====================================== */
  test("[list monoid] All student names") {
    val actual = Raw.query("for (s <- students) yield list s.birthYear", HList("students" -> students)).asInstanceOf[List[Int]]
    val expected = List(1990, 1990, 1989, 1992, 1987, 1992, 1988)
    assert(actual.sorted === expected.sorted)
  }

  test("[set monoid] All student names") {
    val actual = Raw.query("for (s <- students) yield set s.birthYear", HList("students" -> students))
    val expected = Set(1990, 1989, 1987, 1992, 1988)
    assert(actual === expected)
  }

  test("[bag monoid] All student names") {
    val actual = Raw.query("for (s <- students) yield bag s.birthYear", HList("students" -> students))
    val expected = ImmutableMultiset.of(1990, 1990, 1989, 1992, 1987, 1992, 1988)
    assert(actual === expected)
  }

  test("[set monoid] professors") {
    val actual = Raw.query("for (d <- profs) yield set d", HList("profs" -> profs)).asInstanceOf[Set[Professor]]
    logger.info("Result: " + actual)
    assert(actual === ReferenceTestData.profs.toSet)
  }

  test("[set monoid] professors as list") {
    val actual = Raw.query("for (d <- profs) yield list d", HList("profs" -> profs)).asInstanceOf[List[Professor]]
    logger.info("Result: " + actual)
    assert(actual.sorted === ReferenceTestData.profs.sorted)
  }

  /* ======================================
   Predicates
  ====================================== */
  test("set of students born in 1990") {
    assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""", HList("students" -> students)) === Set("Student1", "Student2"))
  }

  test("number of students born in 1992") {
    assert(Raw.query( """for (d <- students; d.birthYear = 1992) yield sum 1""", HList("students" -> students)) === 2)
  }

  test("number of students born before 1991 (included)") {
    assert(Raw.query( """for (d <- students; d.birthYear <= 1991) yield sum 1""", HList("students" -> students)) === 5)
  }

  test("set of students in BC123") {
    assert(Raw.query( """for (d <- students; d.office = "BC123") yield set d.name""", HList("students" -> students)) === Set("Student1", "Student3", "Student5"))
  }

  test("set of students in dep2") {
    assert(Raw.query( """for (d <- students; d.department = "dep2") yield set d.name""", HList("students" -> students)) === Set("Student2", "Student4"))
  }

  test("number of students in dep1") {
    assert(Raw.query( """for (d <- students; d.department = "dep1") yield sum 1""", HList("students" -> students)) === 3)
  }

  test("set of department (using only students table)") {
    assert(Raw.query( """for (s <- students) yield set s.department""", HList("students" -> students)) === Set("dep1", "dep2", "dep3"))
  }


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
}
