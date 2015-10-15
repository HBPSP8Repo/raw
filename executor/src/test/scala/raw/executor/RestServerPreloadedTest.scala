package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.{ParserError, SemanticErrors, TestScanners}


/* Tests: The goal is to test the server layer, not the query engine.
  Schemas
  [OK] - List schemas with: zero, one and two registered files

  Register file
  - Register a file that does not exist
  - Register a file for a schema that already exists.
  - Register a file with a schema name that does not match the filename

  Query
  - A (simple) query returning: primitive type, list, ...

Error conditions:
  Schemas
  - List schemas for a non-existing user

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


class RestServerPreloadedTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  import RestServerPreloadedTest._

  override def beforeAll() = {
    super.beforeAll()
    loadTestData()
  }

  private[this] def loadTestData(): Unit = {
    logger.info("Loading test data")
    clientProxy.registerLocalFile(TestUserJoe, TestScanners.authorsPath)
    clientProxy.registerLocalFile(TestUserJoe, TestScanners.publicationsPath)
    clientProxy.registerLocalFile(TestUserJoe, TestScanners.patientsPath)
    clientProxy.registerLocalFile(TestUserJane, TestScanners.studentsPath)
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

  test("query: primitive result - count(authors)") {
    testQuery(TestUserJoe, "count(authors)", "50")
  }

  test("query: primitive result - count(publications)") {
    testQuery(TestUserJoe, "count(publications)", "1000")
  }

  test("query: primitive result - count(patients)") {
    testQuery(TestUserJoe, "count(patients)", "100")
  }

  test("query: primitive result - count(students)") {
    testQuery(TestUserJane, "count(students)", "7")
  }

  test("query: count(students) wrong user") {
    testQueryFailsWithSemanticError(TestUserJane, "count(authors)")
  }

  test("query: json array - distinct author titles") {
    testQuery(TestUserJoe, "select distinct p.title from authors p", """[ "PhD", "assistant professor", "professor", "engineer" ]""")
  }

  test("query: json array of maps - authors map title counts") {
    testQuery(TestUserJoe, "select distinct p.title as title, count(partition) as number from authors p group by p.title ",
      """[ {
        |  "title" : "professor",
        |  "number" : 18
        |}, {
        |  "title" : "assistant professor",
        |  "number" : 11
        |}, {
        |  "title" : "PhD",
        |  "number" : 16
        |}, {
        |  "title" : "engineer",
        |  "number" : 5
        |} ]""".stripMargin)
  }


  test("query: fail with parser error") {
    testQueryFailsWithParserError(TestUserJoe, "selec tstudents")
  }


}
