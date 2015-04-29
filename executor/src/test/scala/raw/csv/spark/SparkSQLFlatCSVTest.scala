package raw.csv.spark

import com.google.common.collect.ImmutableMultiset
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import raw.csv.spark.AbstractSparkFlatCSVTest

import scala.language.existentials
import scala.reflect.ClassTag

class SparkSQLFlatCSVTest extends AbstractSparkFlatCSVTest {
  val sqlContext = new SQLContext(rawSparkContext.sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  val profsDF: DataFrame = testData.profs.toDF()
  profsDF.show()
  val studentsDF: DataFrame = testData.students.toDF()
  studentsDF.show()
  val departmentsDF: DataFrame = testData.departments.toDF()
  departmentsDF.show()

  // Register tables with sql context to use with the SQL queries
  profsDF.registerTempTable("profs")
  studentsDF.registerTempTable("students")
  departmentsDF.registerTempTable("departments")

  test("Spark SQL") {
    println("Professors:")
    profsDF.show()

    println("Students:")
    studentsDF.show()

    println("Departments:")
    departmentsDF.show()

    println("Students birthyear plus 1")
    studentsDF.select(studentsDF("name"), studentsDF("birthYear") + 1).show()
  }

  test("number of professors") {
    //    assert(Raw.query("for (d <- profs) yield sum 1", HList("profs" -> profs)) === 3)
    assert(profsDF.count() === 3)

    val resDF: DataFrame = sqlContext.sql("SELECT COUNT(*) FROM profs")
    val count = resDF.first().get(0)
    assert(count === 3)
  }

  test("bag of student birth years") {
    //      val actual = Raw.query( """for (d <- students) yield bag d.birthYear""", HList("students" -> students))
    val rows: Array[Row] = studentsDF.select("birthYear").collect()
    val ages: Array[Int] = rows.map(row => row.getInt(0))
    val actual = ImmutableMultiset.copyOf(scala.collection.JavaConversions.asJavaIterable(ages))
    println("Actual: " + actual)
    val expected = ImmutableMultiset.of(1990, 1990, 1989, 1992, 1987, 1992, 1988)
    assert(actual === expected)
  }

  test("[DF] set of students born in 1990") {
    //      assert(Raw.query( """for (d <- students; d.birthYear = 1990) yield set d.name""", HList("students" -> students)) === Set("Student1", "Student2"))
    val rows: DataFrame = studentsDF.filter(studentsDF("birthYear") === 1990).select("name")
    val res = toList[String](rows)
    println("Rows: " + rows + " " + res.mkString(", "))
  }

  test("[SQL] set of students born in 1990") {
    val rows: DataFrame = sqlContext.sql("SELECT name FROM students WHERE birthYear = 1990")
    val actual = toSet[String](rows)
    println("Rows: " + rows + " " + actual.mkString(", "))
  }

  def toList[T: ClassTag](df: DataFrame): List[T] = {
    df.map(_.getAs[T](0)).collect().toList
  }

  def toSet[T: ClassTag](df: DataFrame): Set[T] = {
    df.map(_.getAs[T](0)).collect().toSet
  }

  def toMap[K, V](df: DataFrame): Map[K, V] = {
    df.map(r => (r.getAs[K](0), r.getAs[V](1))).collect().toMap
  }

  test("[SQL] set of students in BC123") {
    //      assert(Raw.query( """for (d <- students; d.office = "BC123") yield set d.name""", HList("students" -> students)) === Set("Student1", "Student3", "Student5"))
    val rows: DataFrame = sqlContext.sql( """SELECT name FROM students WHERE office = "BC123" """)
    val actual = toSet[String](rows)
    val expected = Set("Student1", "Student3", "Student5")
    assertResult(expected)(actual)
  }

  test("[SQL] set of department (using only students table)") {
    //    assert(Raw.query( """for (s <- students) yield set s.department""", HList("students" -> students)) === Set("dep1", "dep2", "dep3"))
    val rows = sqlContext.sql( """ SELECT DISTINCT department FROM students """)
    val actual = toList[String](rows)
    val expected = List("dep1", "dep2", "dep3")
    assertResult(expected)(actual)
  }

  test("[DF] set of department and the headcount (using only students table)") {
    //      val r = Raw.query( """
    //          for (d <- (for (s <- students) yield set s.department))
    //            yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
    //        HList("students" -> students))
    val expected = Map("dep1" -> 3, "dep2" -> 2, "dep3" -> 2)
    val rowsDF = studentsDF.groupBy("department").count()
    val resDF = toMap[String, Int](rowsDF)
    assertResult(expected)(resDF)
  }

  test("[SQL] set of department and the headcount (using only students table)") {
    val expected = Map("dep1" -> 3, "dep2" -> 2, "dep3" -> 2)
    val rows = sqlContext.sql(
      """
        |SELECT department, COUNT(*)
        |FROM students
        |GROUP BY department""".stripMargin)
    val res = toMap[String, Int](rows)
    assertResult(expected)(res)
  }

  test("[DF] set of department and the headcount (using both departments and students table)") {
    //      val r = Raw.query("""
    //          for (d <- (for (d <- departments) yield set d.name))
    //              yield set (name := d, count := (for (s <- students; s.department = d) yield sum 1))""",
    //        HList("departments" -> departments, "students" -> students))
    val expected = Map("dep1" -> 3, "dep2" -> 2, "dep3" -> 2)

    val rows = studentsDF
      .join(departmentsDF, studentsDF("department") === departmentsDF("name"))
      .groupBy(departmentsDF("name")).count()
    val actual = toMap[String, Int](rows)
    assertResult(expected)(actual)
  }

  test("[SQL] set of department and the headcount (using both departments and students table)") {
    val expected = Map("dep1" -> 3, "dep2" -> 2, "dep3" -> 2)
    val rows = sqlContext.sql(
      """
        |SELECT d.name, COUNT(s.name)
        |FROM departments AS d, students AS s
        |WHERE d.name = s.department
        |GROUP BY d.name""".stripMargin)

    val actual = toMap[String, Int](rows)
    assertResult(expected)(actual)
  }

//    test("[SQL] most studied discipline") {
////      val r = Raw.query("""
////          for (t <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number =
////          (for (x <- for (d <- departments) yield set (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1))) yield max x.number)) yield set t.name""",
////        HList("departments" -> departments, "students" -> students))
//
//      val rows = sqlContext.sql(
//      """
//        |SELECT discipline, MAX(*)
//        |FROM (SELECT d.discipline, COUNT(*)
//        | FROM departments AS d, students AS s
//        | WHERE d.name = s.department
//        | GROUP BY d.discipline) AS t
//        |GROUP BY discipline
//      """.stripMargin
//      )
//      rows.show()
////      assert(r.size === 1)
////      assert(r === Set("Computer Architecture"))
//    }

  import org.apache.spark.sql.functions._

  test("[DF] most studied discipline") {
    // Group by disciplines with count of students
    val rows = studentsDF
      .join(departmentsDF, studentsDF("department") === departmentsDF("name"))
      .groupBy(departmentsDF("discipline")).count()
    rows.show()

    // Self joins are broken in Spark 1.3.X https://issues.apache.org/jira/browse/SPARK-6247
//    val r2 = rows.as("r2")
//    val r1 = rows.as("r1")
//    val r = r1.join(r2)
//    rows.join(rows.as("rows2"), rows("count") === $"max(rows2.count)").show()

    // select max
    val maxCount = rows.select(max("count")).collect().head.getLong(0)
    val rows2 = rows.where(rows("count") === maxCount).select("discipline")
    val actual = rows2.collect().head.getString(0)
    println(actual)
    assertResult("Computer Architecture")(actual)
  }

  //  test("list of disciplines which have three students") {
  //    val r = Raw.query(
  //      """for (t <- for (d <- departments) yield list (name := d.discipline, number := (for (s <- students; s.department = d.name) yield sum 1)); t.number = 3) yield list t.name""",
  //      HList("departments" -> departments, "students" -> students))
  //
  //    assert(r.size === 1)
  //    assert(r === List("Computer Architecture"))
  //  }
  //
  //  test("set of the number of students per department") {
  //    val r = Raw.query(
  //      """for (d <- students) yield set (name := d.department, number := for (s <- students; s.department = d.department) yield sum 1)""",
  //      HList("students" -> students))
  //
  //    assert(r.size === 3)
  //    val mr = r.map { case v => Map("name" -> v.name, "number" -> v.number) }
  //    assert(mr === Set(Map("name" -> "dep1", "number" -> 3), Map("name" -> "dep2", "number" -> 2), Map("name" -> "dep3", "number" -> 2)))
  //  }

  //  test("set of names departments which have the highest number of students", ???, Set("dep1"))
  //  test("set of names departments which have the lowest number of students", ???, Set("dep2", "dep3"))
  //  test("set of profs which have the highest number of students in their department", ???, Set("Prof1"))
  //  test("set of profs which have the lowest number of students in their department", ???, Set("Prof2", "Prof3"))
  //  test("set of students who study 'Artificial Intelligence'", ???, Set("Student6", "Student7"))
}

