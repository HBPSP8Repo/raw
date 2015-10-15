package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestScanners
import raw.utils.RawUtils

/*
 * Register file:
 * - non-existing file
 * - wrong contents
*/
class RestServerEmptyStorageTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  // Support for CSV files with no header. The columns should be given names like v1, v2...
  test("CSV register && query: students, no file header") {
    clientProxy.registerLocalFile(RestServerPreloadedTest.TestUserJoe, TestScanners.studentsNoHeaderPath, "studentsnoheader")
    val resp: String = clientProxy.query("select s.v1 from studentsnoheader s", RestServerPreloadedTest.TestUserJoe)
    assert(resp == """[ "Student1", "Student2", "Student3", "Student4", "Student5", "Student6", "Student7" ]""")
  }

  // Same csv file, with headers
  test("CSV register && query: students, with file header") {
    clientProxy.registerLocalFile(RestServerPreloadedTest.TestUserJoe, TestScanners.studentsPath, "studentsheader")
    val resp: String = clientProxy.query("select s.Name from studentsheader s", RestServerPreloadedTest.TestUserJoe)
    assert(resp == """[ "Student1", "Student2", "Student3", "Student4", "Student5", "Student6", "Student7" ]""")
  }

  test("register: malformed url") {
    try {
      clientProxy.registerFile("http", """foo-bar""", "badurlfilename.json", "badurlSchema", "json", RestServerPreloadedTest.TestUserJoe)
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }

  test("register: file does not exist") {
    try {
      clientProxy.registerFile("http", """http://rawlabs.com/doesnnotexist.json""", "doesnnotexist.json", "dummySchema", "json", RestServerPreloadedTest.TestUserJoe)
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }

  test("register: file with invalid contents.") {
    try {
      clientProxy.registerLocalFile(RestServerPreloadedTest.TestUserJoe, RawUtils.toPath("data/badfile.json"))
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }
}
