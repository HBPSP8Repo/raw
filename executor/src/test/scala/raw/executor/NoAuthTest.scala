package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestDatasources

class NoAuthTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  override def beforeAll() = {
    System.setProperty("raw.demo-mode", "enabled")
    super.beforeAll()
  }

  override def afterAll() = {
    super.afterAll()
    System.clearProperty("raw.demo-mode")
  }

  // Not a proper test, just useful for debugging.
  test("noauth") {
    val res = clientProxy.getSchemas(DropboxAuthUsers.TestUserJoe)
  }
}
