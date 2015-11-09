package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestScanners

class RestServerNoAuthTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  override def beforeAll() = {
    System.setProperty("raw.authentication", "disabled")
    super.beforeAll()
  }

  override def afterAll() = {
    System.clearProperty("raw.authentication")
  }

  test("noauth") {
    val res = clientProxy.getSchemas(DropboxAuthUsers.TestUserJoe)
  }
}
