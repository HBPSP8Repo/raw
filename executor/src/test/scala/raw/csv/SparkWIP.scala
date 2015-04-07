package raw.csv

import raw.Raw
import shapeless.HList

class SparkWIP extends AbstractSparkFlatCSVTest {

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

  // Needs Nest
  test("set of department and the headcount (using only students table)") {
    val r = Raw.query( """
          for (d <- (for (s <- students) yield set s.department))
            yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
      HList("students" -> students))
    println(r)
    val mr = r.map(_.toMap)

    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  }
}
