package raw.csv.spark

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.csv.{Department, Student, Professor}
import raw.repl.RawSparkContext
import raw.util.CSVToRDDParser

import scala.language.reflectiveCalls

class TestData(csvParser: CSVToRDDParser) {
  val profs = csvParser.parse("data/profs.csv", l => Professor(l(0), l(1)))
  val students = csvParser.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
  val departments = csvParser.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))
}

// TODO: See SharedSparkContext in Spark project for an alternative refactoring of this base class.
abstract class AbstractSparkFlatCSVTest extends FunSuite with StrictLogging with BeforeAndAfterAll  {
  /* Create a new Spark context for every test suite.
  This allows us to use the afterAll() callback to close Spark at the end of the test suit.
  An alternative design would be to use a single Spark context for all the test run, by creating it inside an object.
  The problem with this design is how to ensure that the Spark context is always closed at the end of all the tests,
  AFAIK, there is no callback in the ScalaTests to execute code when the tests end. Another problem is the loss of
  independence between test suites.
  The first problem can be mitigated by ensuring that the JVM is always closed at the end of the tests (fork := true)
  in SBT.
  */
  val rawSparkContext = new RawSparkContext
  val csvParser = new CSVToRDDParser(rawSparkContext.sc)
  val testData = new TestData(csvParser)

  /*
   If no tests are executed in a suit, ScalaTest will, by default, not execute the afterAll() hook.
   https://groups.google.com/forum/#!topic/scalatest-users/M9shKbNecvA
   But because the Spark context is created in the constructor of the object, it is always created so we must force the
   afterAll to also be always executed. An alternative is to use beforeAll to initialize the Spark context, so that if
   all tests in a suite are disabled, both initialization and cleanup are skipped. But this increases the boilerplate
   (must declare var fields initially set to null then assign them on the beforeAll) and requires all subclasses to also
   use beforeAll for their initialization.
   */
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  override def afterAll(): Unit = {
    rawSparkContext.close()
  }
}
