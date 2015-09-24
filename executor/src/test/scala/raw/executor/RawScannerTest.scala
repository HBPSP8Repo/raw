package raw.executor

import java.nio.file.{Path, Paths}
import java.util

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.rest.RawRestServer
import raw.utils.RawUtils

import scala.concurrent.Await
import scala.concurrent.duration._

class RawScannerTest extends FunSuite with StrictLogging with BeforeAndAfterAll {
  var restServer: RawRestServer = _
  var testDir: Path = _
  val httpclient = HttpClients.createDefault()

  override def beforeAll() = {
    testDir = RawUtils.getTemporaryDirectory("test-basedata")
    RawUtils.cleanOrCreateDirectory(testDir)
    restServer = new RawRestServer("scala", Some(testDir.toString))
    val serverUp = restServer.start()
    logger.info("Waiting for rest server to start")
    Await.ready(serverUp, Duration(2, SECONDS))
  }

  override def afterAll() = {
    restServer.stop()
    httpclient.close()
  }
}
