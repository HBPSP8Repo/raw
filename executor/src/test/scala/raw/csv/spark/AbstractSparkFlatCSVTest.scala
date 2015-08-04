//package raw.csv.spark
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.spark.rdd.RDD
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import raw.SharedSparkContext
//import raw.csv.{Department, Professor, Student}
//import raw.util.CSVToRDDParser
//
//abstract class AbstractSparkFlatCSVTest extends FunSuite with StrictLogging with BeforeAndAfterAll with SharedSparkContext {
//  var profs: RDD[Professor] = _
//  var students: RDD[Student] = _
//  var departments: RDD[Department] = _
//
//  override def beforeAll() {
//    super.beforeAll()
//    val csvParser = new CSVToRDDParser(sc)
//    profs = csvParser.parse("data/profs.csv", l => Professor(l(0), l(1)))
//    students = csvParser.parse("data/students.csv", l => Student(l(0), l(1).toInt, l(2), l(3)))
//    departments = csvParser.parse("data/departments.csv", l => Department(l(0), l(1), l(2)))
//  }
//
//  //
//  //  /*
//  //   If no tests are executed in a suit, ScalaTest will, by default, not execute the afterAll() hook.
//  //   https://groups.google.com/forum/#!topic/scalatest-users/M9shKbNecvA
//  //   But because the Spark context is created in the constructor of the object, it is always created so we must force the
//  //   afterAll to also be always executed. An alternative is to use beforeAll to initialize the Spark context, so that if
//  //   all tests in a suite are disabled, both initialization and cleanup are skipped. But this increases the boilerplate
//  //   (must declare var fields initially set to null then assign them on the beforeAll) and requires all subclasses to also
//  //   use beforeAll for their initialization.
//  //   */
//  //  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
//  //
//  //  override def afterAll(): Unit = {
//  //    rawSparkContext.close()
//  //  }
//}
