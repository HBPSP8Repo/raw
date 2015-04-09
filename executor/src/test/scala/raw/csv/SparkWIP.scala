package raw.csv

import org.apache.spark.rdd.RDD
import raw.Raw
import shapeless.HList

case class Employee(name: String, deptID: String)

case class Dept(id: String, name: String)

object SparkWIP {
  def convertNulls(v: String): String = {
    if (v.equalsIgnoreCase("null")) {
      null.asInstanceOf[String]
    } else {
      v
    }
  }
}

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
//  test("set of department and the headcount (using only students table)") {
//    val r = Raw.query(
//      """for (d <- (for (s <- students) yield set s.department))
//                    yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
//      HList("students" -> students))
//    println(r)
//  }

  //  // Needs Nest
  //  test("set of department and the headcount (using only students table)") {
  //    val r = Raw.query( """
  //          for (d <- (for (s <- students) yield set s.department))
  //            yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
  //      HList("students" -> students))
  //    println(r)
  //    val mr = r.map(_.toMap)

  //    val f = (arg:Tuple2[String, String]) => arg._1 + arg._2
  //    f.apply(("aa", "bb"))
  //    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  //  }

  def toString[R](rdd: RDD[R]): String = {
    rdd.collect().toList.mkString("\n");
  }

  val employees = csvReader.parse("data/wikipedia/employee.csv", l => Employee(l(0), SparkWIP.convertNulls(l(1))))
  val depts = csvReader.parse("data/wikipedia/department.csv", l => Dept(SparkWIP.convertNulls(l(0)), l(1)))


  // Number of students per department
  //  for (d <- (for (s <- students) yield set s.department))
  //  yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))

  ignore("left outer join") {
    //    println("Employees:\n" + toString(employees))
    //    println("Departments:\n" + toString(depts))

    val matching: RDD[(Employee, Dept)] = employees.cartesian(depts).filter({
      case (emp, dept) => if (emp.deptID == null) false else emp.deptID.equals(dept.id)
    }
    )
    //    println("Matching:\n" + toString(matching))

    val resWithOption: RDD[(Employee, (Employee, Option[Dept]))] = employees.map(v => (v, v)).leftOuterJoin(matching)
    //    println("resWithOption:\n" + toString(resWithOption))

    val res: RDD[(Employee, Dept)] = resWithOption.map({
      case (v1, (v2, None)) => (v1, null)
      case (v1, (v2, Some(w))) => (v1, w)
    })

    println("Left outer join:\n" + toString(res))

  }


  test("nest") {
    //      spark_outer_join(
    //        arg._2.department = arg._1.department,
    //        spark_select(true, spark_scan("students")),
    //        spark_select(true, spark_scan("students")))))

    println("Students:\n" + toString(students))
    val matching: RDD[(Student, Student)] = students.cartesian(students).filter({
      case (s1, s2) => if (s1.department == null) false else s1.department.equals(s2.department)
    }
    )
    //    println("Matching:\n" + toString(matching))

    val resWithOption: RDD[(Student, (Student, Option[Student]))] = students.map(v => (v, v)).leftOuterJoin(matching)
    //    println("resWithOption:\n" + toString(resWithOption))

    val nestInput: RDD[(Student, Student)] = resWithOption.map({
      case (v1, (v2, None)) => (v1, null)
      case (v1, (v2, Some(w))) => (v1, w)
    })

    println("Left outer join:\n" + toString(nestInput))

    /* Nest.
    Input: RDD[(Employee, Dept)]
    m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp,

    All expressions take (Student, Student) as input.
    Input
            sum : Monoid
            1 : Exp e(w) => Int
            arg._1 : Exp f(w) => Student
            true : Exp p(w) => Boolean
            arg._2 : Exp, g(w) => Student
            spark_outer_join

     f: group-by function
     e: map function
     p: filtering predicate
     g: null checking
      */

    //    val f = (emp: Employee, dept: Dept) => emp
    val e = (arg: Tuple2[Student, Student]) => 1
    val f = (arg: Tuple2[Student, Student]) => arg._1
    val p = (arg: Tuple2[Student, Student]) => true
    val g = (arg: Tuple2[Student, Student]) => arg._2

    // ([Employee, Dept]) => Employee
    // TODO: Is this equivalent to obtain a set of the keys?
    val r1: RDD[(Student, Iterable[(Student, Student)])] = nestInput.groupBy(f)
    println("groupBy(f(a)):\n" + toString(r1))

    val pif: RDD[Student] = r1.keys.distinct
    println("pif:\n" + toString(pif))

    val afterP: RDD[(Student, Student)] = nestInput.filter(p)
    println("afterP:\n" + toString(afterP))

    val afterG: RDD[(Student, Student)] = afterP.filter(w => g(w) != null)
    println("afterP:\n" + toString(afterG))

    val cross: RDD[(Student, (Student, Student))] = pif.cartesian(afterG)
    println("cross:\n" + toString(cross))

    val dotEquality: RDD[(Student, (Student, Student))] = cross.filter(arg => arg._1 == f(arg._2))
    println("dotEquality:\n" + toString(dotEquality))

    val r: RDD[(Student, Iterable[Student])] = dotEquality.keys.groupBy(f => f)
    println("r:\n" + toString(r))

    val res = r.mapValues(iter => iter.size)
    println("res:\n" + toString(res))


    
  }
}