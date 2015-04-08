package raw.csv

import org.apache.spark.rdd.RDD
import raw.Raw
import raw.util.CSVToRDDParser
import shapeless.HList

case class Employee(name:String, deptID:String)
case class Dept(id:String, name:String)

object SparkWIP {
  def convertNulls(v:String): String= {
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

//    val f = (arg:Tuple2[String, String]) => arg._1 + arg._2
//    f.apply(("aa", "bb"))
    assert(mr === Set(Map("name" -> "dep1", "count" -> 3), Map("name" -> "dep2", "count" -> 2), Map("name" -> "dep3", "count" -> 2)))
  }

//  def toString[R](rdd:RDD[R]): String = {
//    rdd.collect().toList.mkString("\n");
//  }
//
//  val employees = csvReader.parse("data/wikipedia/employee.csv", l => Employee(l(0), SparkWIP.convertNulls(l(1))))
//  val depts = csvReader.parse("data/wikipedia/department.csv", l => Dept(SparkWIP.convertNulls(l(0)), l(1)))
//
//  test("left outer join") {
//    println("Starting test")
//    println("Employees:\n" + toString(employees))
//    println("Departments:\n" + toString(depts))
//
//    val matching: RDD[(Employee, Dept)] = employees.cartesian(depts).filter({
//      case (emp, dept) => if (emp.deptID == null) false else  emp.deptID.equals(dept.id) }
//    )
//    println("Matching:\n" + toString(matching))
//
//    val resWithOption: RDD[(Employee, (Employee, Option[Dept]))] =  employees.map(v => (v, v)).leftOuterJoin(matching)
//    println("resWithOption:\n" + toString(resWithOption))
//
//    val res: RDD[(Employee, Dept)] = resWithOption.map( {
//      case (v1, (v2, None)) => (v1, null)
//      case (v1, (v2, Some(w))) => (v1, w)
//    })
//
//    println("Result:\n" + toString(res))
//  }

}
