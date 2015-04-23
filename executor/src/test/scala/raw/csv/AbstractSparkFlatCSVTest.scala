package raw.csv

import com.google.common.collect.ImmutableMultiset
import com.typesafe.scalalogging.{StrictLogging, LazyLogging}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.Raw
import raw.repl.RawSparkContext
import raw.util.CSVToRDDParser
import shapeless.HList

import scala.language.reflectiveCalls

abstract class AbstractSparkFlatCSVTest extends FunSuite with StrictLogging with BeforeAndAfterAll {
  val csvParser = new CSVToRDDParser with RawSparkContext

  val profs = csvParser.parse("data/profs.csv", l => Professor(l(0), l(1)))
  val students = csvParser.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val departments = csvParser.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))

  override def afterAll(): Unit = {
    csvParser.close()
  }
}
