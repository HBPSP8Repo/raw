package raw.executor

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}
import raw.rest.RawRestServer
import raw.spark.DefaultSparkConfiguration
import raw.utils.RawUtils
import raw.{ParserError, SemanticErrors, TestScanners}

import scala.concurrent.Await
import scala.concurrent.duration._


/* Tests: The goal is to test the server layer, not the query engine.
  Schemas
  - List schemas with: zero, one and two registered files

  Register file
  - Register a file that does not exist
  - Register a file for a schema that already exists.
  - Register a file with a schema name that does not match the filename

  Query
  - A (simple) query returning: primitive type, list, ...

Error conditions:
  Schemas
  - List schemas for a non-existing user

  Register file:
  - non-existing file
  - wrong contents

  Query: return 400 plus error information
  - non-existing schema
  - syntax error
  - semantic error
 */
object RestServerPreloadedTest {
  final val TestUserJoe = "123456789-Joe_Test_Doe"
  final val TestUserJane = "987654321-Jane_Test_Dane"
  final val TestUserNonexisting = "000000-Elvis"
}


class RestServerPreloadedTest extends FunSuite with RawRestService with StrictLogging with BeforeAndAfterAll {

  import RestServerPreloadedTest._

  override def beforeAll() = {
    super.beforeAll()
    loadTestData()
  }

  def loadTestData(): Unit = {
    logger.info("Loading test data")
    clientProxy.registerFile(TestUserJoe, TestScanners.authorsPath)
    clientProxy.registerFile(TestUserJoe, TestScanners.publicationsPath)
    clientProxy.registerFile(TestUserJoe, TestScanners.patientsPath)
    clientProxy.registerFile(TestUserJane, TestScanners.studentsPath)
  }

  def schemasTest(user: String, expected: Set[String]) = {
    val actual = clientProxy.getSchemas(user)
    assert(actual === expected)
  }

  test("list schemas: three schemas") {
    schemasTest(TestUserJoe, Set("authors", "publications", "patients"))
  }

  test("list schemas: one schema") {
    schemasTest(TestUserJane, Set("students"))
  }

  test("list schemas: no schema") {
    schemasTest(TestUserNonexisting, Set())
  }

  def testQuery(user: String, query: String, expected: String) = {
    val actual = clientProxy.query(query, user)
    assert(actual === expected)
  }

  def testQueryFails(user: String, query: String, expectedErrorClass: Class[_]) = {
    try {
      val actual = clientProxy.query(query, user)
      fail(s"Query should have failed. Instead succeeded with result:\n$actual")
    } catch {
      case ex: CompilationException =>
        val qe = ex.queryError
        logger.info(s"Query error: $qe")
        assert(ex.queryError.getClass === expectedErrorClass)
    }
  }

  def testQueryFailsWithSemanticError(user: String, query: String) = {
    testQueryFails(user, query, classOf[SemanticErrors])
  }

  def testQueryFailsWithParserError(user: String, query: String) = {
    testQueryFails(user, query, classOf[ParserError])
  }

  test("query: count(authors)") {
    testQuery(TestUserJoe, "count(authors)", "50")
  }

  test("query: count(publications)") {
    testQuery(TestUserJoe, "count(publications)", "1000")
  }

  test("query: count(patients)") {
    testQuery(TestUserJoe, "count(patients)", "100")
  }

  test("query: count(students)") {
    testQuery(TestUserJane, "count(students)", "7")
  }

  test("query: count(students) wrong user") {
    val qe = testQueryFailsWithSemanticError(TestUserJane, "count(authors)")
  }

  test("query: fail with parser error") {
    val qe = testQueryFailsWithParserError(TestUserJoe, "selec tstudents")
  }

  ignore("schemas") {
    val schemas = clientProxy.getSchemas("joedoe")
    assert(schemas.isEmpty, s"Expected emoty schemas. Found: $schemas")
    val name = clientProxy.registerFile("joedoe", TestScanners.patientsPath)
    logger.info(s"Registered schema: $name")

    val count = clientProxy.query("count(patients)", "joedoe")
    logger.info(s"Count(patients): $count")

    val schemasAfter = clientProxy.getSchemas("joedoe")
    val expected = List("patients")
    assert(schemasAfter == expected, s"Expected $expected. Found: $schemasAfter")
  }

  //  test("Large") {
  //    val rawUser = "joedoe"
  //    val storageManager = restServer.rawServer.storageManager
  //
  //    val schemas = storageManager.listUserSchemas(rawUser)
  //    logger.info("Found schemas: " + schemas.mkString(", "))
  //    val scanners: Seq[RawScanner[_]] = schemas.map(name => storageManager.getScanner(rawUser, name))
  //    var i = 0
  //    while (i < 5) {
  //      val result = CodeGenerator.query(studentsHeaderPlan, scanners)
  //      logger.info("Result: " + result)
  //      i += 1
  //    }
  //  }


}
