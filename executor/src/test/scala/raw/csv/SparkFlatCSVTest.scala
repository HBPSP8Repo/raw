package raw.csv

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import raw.Raw
import raw.util.CSVToRDDParser
import shapeless.HList

class SparkFlatCSVTest extends FunSuite with LazyLogging {

  case class TestRDDs(students: RDD[Student], profs: RDD[Professor], departments: RDD[Department])

  lazy val testRDDs: TestRDDs = {
    val csvReader = new CSVToRDDParser()
    try {
      new TestRDDs(
        csvReader.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3))),
        csvReader.parse("data/profs.csv", l => Professor(l(0), l(1))),
        csvReader.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))
      )
    } finally {
      csvReader.close()
    }
  }

  // Disabled until RDD executor is implemented
//  test("test RDD") {
//    val actual = Raw.query("for (d <- profs) yield sum 1", HList("profs" -> testRDDs.profs))
//    logger.info("\nResult: " + actual)
//    assert(actual === 3)
//  }

}