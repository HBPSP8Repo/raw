package raw.executor

import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Suite, BeforeAndAfterAll}
import raw.rest.{RealDropboxClient, RawRestServer}
import raw.spark.DefaultSparkConfiguration
import raw.utils.RawUtils

import scala.concurrent.Await
import scala.concurrent.duration._

// Mixable trait to start and stop a RawService for tests.
trait RawRestServerContext extends BeforeAndAfterAll with StrictLogging {
  self: Suite =>

  val testDir: Path = RawUtils.getTemporaryDirectory("it-test-data")
  val clientProxy = new RawServiceClientProxy
  var restServer: RawRestServer = _

  // Override to modify configuration
  var conf = DefaultSparkConfiguration.conf

  override def beforeAll() = {
    RawUtils.cleanOrCreateDirectory(testDir)
    restServer = new RawRestServer("scala", Some(testDir.toString)) with TestDropboxClient
    val serverUp = restServer.start()
    logger.info("Waiting for RAW rest server to start")
    Await.ready(serverUp, Duration(2, SECONDS))
  }

  override def afterAll() = {
    restServer.stop()
  }

}
