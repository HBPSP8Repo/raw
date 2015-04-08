package raw.csv

import com.google.common.collect.ImmutableMultiset
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.Raw
import raw.util.CSVToRDDParser
import shapeless.HList

import scala.language.reflectiveCalls

abstract class AbstractSparkFlatCSVTest extends FunSuite with LazyLogging with BeforeAndAfterAll {
  val csvReader = new CSVToRDDParser()

  val profs = csvReader.parse("data/profs.csv", l => Professor(l(0), l(1)))
  val students = csvReader.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val departments = csvReader.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))

  override def afterAll(): Unit = {
    csvReader.close()
  }
}
