package raw.experiments

import org.apache.spark.rdd.RDD
import raw._
import raw.algebra.Expressions._
import raw.csv.Student
import raw.csv.spark.AbstractSparkFlatCSVTest

import scala.reflect.ClassTag

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
    rdd.collect().toList.mkString("\n")
  }

  val employees = csvParser.parse("data/wikipedia/employee.csv", l => Employee(l(0), SparkWIP.convertNulls(l(1))))
  val depts = csvParser.parse("data/wikipedia/department.csv", l => Dept(SparkWIP.convertNulls(l(0)), l(1)))

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

  //  for (d <- (for (s <- students) yield set s.department))
  //  yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1)

  // Result:
  //    Set(_n303007585(dep1,3), _n303007585(dep3,2), _n303007585(dep2,2))


  //    spark_reduce(
  //      set,
  //      record("name" -> arg._1.department, "count" -> arg._2),
  //      true,
  //      spark_nest(
  //        sum,
  //        1,
  //        arg._1,
  //        true,
  //        arg._2,
  //        spark_outer_join(
  //          arg._2.department = arg._1.department,
  //          spark_select(true, spark_scan("students")),
  //          spark_select(true, spark_scan("students")))))

  def outerJoin[T >: Null <: AnyRef : ClassTag](left: RDD[T], right: RDD[T], p: (((T, T)) => Boolean)): RDD[(T, T)] = {
    val matching = left
      .cartesian(right)
      .filter(tuple => tuple._1 != null)
      .filter(p)

    val resWithOption = left
      .map(v => (v, v))
      .leftOuterJoin(matching)

    val r: RDD[(T, T)] = resWithOption.map({
      case (v1, (v2, None)) => (v1, null)
      case (v1, (v2, Some(w))) => (v1, w)
    })

    r
  }

  def zeroOf(m: Monoid) = m match {
    case _: AndMonoid => BoolConst(true)
    case _: OrMonoid => BoolConst(false)
    case _: SumMonoid => IntConst("0")
    case _: MultiplyMonoid => IntConst("1")
    case _: MaxMonoid => IntConst("0") // TODO: Fix since it is not a monoid
  }

  test("nest") {
    println("Students:\n" + toString(testData.students))
    // spark_outer_join
    val matching: RDD[(Student, Student)] = testData.students.cartesian(testData.students).filter({
      case (s1, s2) => if (s1.department == null) false else s1.department.equals(s2.department)
    }
    )
    //    println("Matching:\n" + toString(matching))

    val resWithOption: RDD[(Student, (Student, Option[Student]))] = testData.students.map(v => (v, v)).leftOuterJoin(matching)
    //    println("resWithOption:\n" + toString(resWithOption))

    val nestInput: RDD[(Student, Student)] = resWithOption.map({
      case (v1, (v2, None)) => (v1, null)
      case (v1, (v2, Some(w))) => (v1, w)
    })
    // Pairs of students having the same department
    //    println("Left outer join:\n" + toString(nestInput))

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
    val m = SumMonoid()
    val e = (arg: Tuple2[Student, Student]) => 1
    val f = (arg: Tuple2[Student, Student]) => arg._1
    val p = (arg: Tuple2[Student, Student]) => true
    val g = (arg: Tuple2[Student, Student]) => arg._2

//    val zero: Const with Serializable = zeroOf(m)
    val zero = 0

    // If the underlying collection is not a set (not idempotent), do not call distinct.
    // pif are the elements distinct under dot-equality.
    // we can do group by directly when the collection resulting from applying f is idempotent (set), but it will not work on lists.
    val grouped: RDD[(Student, Iterable[(Student, Student)])] = nestInput.groupBy(f)
    println("groupBy:\n" + toString(grouped))

    val filteredP: RDD[(Student, Iterable[(Student, Student)])] = grouped.map(arg => (arg._1, arg._2.filter(p)))
    println("filteredP:\n" + toString(filteredP))

    //    val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
    //    val map2 = map1.map(v => (v._1, v._2.map(${exp(f1)})))
    //    println("map2:\n" + map2.mkString("\n"))
    // nulls are transformed into the monoid zero. Non-null values are mapped by e.
    val f1 = (x: (Student, Student)) => if (g(x) == null) zero else e(x)
    val mapped: RDD[(Student, Iterable[Int])] = grouped.map(v => (v._1, v._2.map(x => f1(x))))
    println("mapped:\n" + toString(mapped))

    val folded = mapped.map(v => (v._1, v._2.fold(zero)((a, b) => a + b)))
    println("folded:\n" + toString(folded))
    // End of nest

    val ff = folded.map(arg => Tuple2(arg._1.department, arg._2))
    println("ff:\n" + toString(ff))

    val setResult = ff.toLocalIterator.toSet
    println("setResult:\n" + setResult)
  }
}
