package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestDatasources
import raw.utils.RawUtils

/*
 * Register file:
 * - non-existing file
 * - wrong contents
*/
class EmptyStorageTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  // Support for CSV files with no header. The columns should be given names like v1, v2...
  test("CSV register && query: students, no file header") {
    clientProxy.registerLocalFile(DropboxAuthUsers.TestUserJoe, TestDatasources.studentsNoHeaderPath, "studentsnoheader")
    val resp: String = clientProxy.query("select s.v1 from studentsnoheader s", DropboxAuthUsers.TestUserJoe)
    assert(resp == """[ "Student1", "Student2", "Student3", "Student4", "Student5", "Student6", "Student7" ]""")
  }

  // Same csv file, with headers
  test("CSV register && query: students, with file header") {
    clientProxy.registerLocalFile(DropboxAuthUsers.TestUserJoe, TestDatasources.studentsPath, "studentsheader")
    val resp: String = clientProxy.query("select s.Name from studentsheader s", DropboxAuthUsers.TestUserJoe)
    assert(resp == """[ "Student1", "Student2", "Student3", "Student4", "Student5", "Student6", "Student7" ]""")
  }

  test("text file") {
    clientProxy.registerLocalFile(DropboxAuthUsers.TestUserJoe, TestDatasources.httpLogsPathUTF8, "httplogs")
    val resp = clientProxy.query( """select distinct h parse as r"([-.\\w]+).*" from httplogs h""", DropboxAuthUsers.TestUserJoe)
    assert(resp == """[ "in24.inetnebr.com", "uplherc.upl.com", "ix-esc-ca2-07.ix.netcom.com", "slppp6.intermind.net", "piweba4y.prodigy.com", "133.43.96.45", "kgtyk4.kj.yamagata-u.ac.jp", "d0ucr6.fnal.gov" ]""")
  }

  test("text file UTF16") {
    clientProxy.registerLocalFile(DropboxAuthUsers.TestUserJoe, TestDatasources.httpLogsPath, "httplogs")
    val resp = clientProxy.query( """select distinct h parse as r"([-.\\w]+).*" from httplogs h""", DropboxAuthUsers.TestUserJoe)
    assert(resp == """[ "in24.inetnebr.com", "uplherc.upl.com", "ix-esc-ca2-07.ix.netcom.com", "slppp6.intermind.net", "piweba4y.prodigy.com", "133.43.96.45", "kgtyk4.kj.yamagata-u.ac.jp", "d0ucr6.fnal.gov" ]""")
  }

  test("register: malformed url") {
    try {
      clientProxy.registerFile("http", """foo-bar""", "badurlfilename.json", "badurlSchema", "json", DropboxAuthUsers.TestUserJoe)
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }

  test("register: file does not exist") {
    try {
      clientProxy.registerFile("http", """http://rawlabs.com/doesnnotexist.json""", "doesnnotexist.json", "dummySchema", "json", DropboxAuthUsers.TestUserJoe)
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }

  test("register: file with invalid contents.") {
    try {
      clientProxy.registerLocalFile(DropboxAuthUsers.TestUserJoe, RawUtils.toPath("data/badfile.json"))
      fail("Expected exception but register file succeeded")
    } catch {
      case _: ClientErrorWrapperException => // Test pass.
      case ex: Throwable => fail("Wrong type of exception: " + ex)
    }
  }

  test("authorization") {
    val schemas = clientProxy.getSchemas(DropboxAuthUsers.TestUserJoe)
    logger.info("Schemas: " + schemas)
  }
}