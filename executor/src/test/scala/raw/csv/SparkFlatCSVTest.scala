package raw.csv

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.Raw
import raw.util.{CSVParser, CSVToRDDParser}
import shapeless.HList

case class TestRDDs(students: RDD[Student], profs: RDD[Professor], departments: RDD[Department])

class SparkFlatCSVTest extends FunSuite with LazyLogging with BeforeAndAfterAll with ReferenceTestData {
  val csvReader = new CSVToRDDParser()
  val testRDDs: TestRDDs = {
    new TestRDDs(
      csvReader.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3))),
      csvReader.parse("data/profs.csv", l => Professor(l(0), l(1))),
      csvReader.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))
    )
  }

  override def afterAll(): Unit = {
    csvReader.close()
  }

  test("professors count") {
    val actual = Raw.query("for (d <- profs) yield sum 1", HList("profs" -> testRDDs.profs))
    logger.info("Result: " + actual)
    assert(actual === profs.size)
  }

  test("professors as set") {
    val actual = Raw.query("for (d <- profs) yield set d", HList("profs" -> testRDDs.profs)).asInstanceOf[Set[Professor]]
    logger.info("Result: " + actual)
    assert(actual === profs.toSet)
  }

  test("professors as list") {
    val actual = Raw.query("for (d <- profs) yield list d", HList("profs" -> testRDDs.profs)).asInstanceOf[List[Professor]]
    logger.info("Result: " + actual)
    assert(actual.sorted === profs.sorted)
  }
}
