package raw.executor

import java.nio.file.{Path, Paths}

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, Suite}
import raw.rest.RawRestServer
import raw.spark.DefaultSparkConfiguration
import raw.utils.RawUtils
import raw.{ParserError, SemanticErrors}

import scala.concurrent.Await
import scala.concurrent.duration._


trait InferrerConfiguration extends StrictLogging {
  // Set the path to the inferrer. Should be <project root>/inferrer
  private[this] def setInferrerPath() = {
    val cwd = System.getProperty("user.dir")
    // The CWD may be either the top level directory or the executor directory (from sbt console, project executor)
    val inferrerPath = if (cwd.endsWith("executor")) {
      Paths.get(cwd).resolve("../inferrer")
    } else {
      Paths.get(cwd).resolve("inferrer")
    }
    logger.info("Setting inferrer path to: " + inferrerPath)
    System.setProperty("raw.inferrer.path", inferrerPath.toAbsolutePath.toString)
  }

  setInferrerPath()
}

// Mixable trait to start and stop a RawService for tests.
trait RawRestServerContext extends BeforeAndAfterAll with StrictLogging with InferrerConfiguration {
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

  def schemasTest(credentials: RawCredentials, expected: Set[String]): Unit = {
    val actual = clientProxy.getSchemas(credentials)
    assert(actual === expected)
  }

  def testQuery(credentials: RawCredentials, query: String, expected: String): Unit = {
    val actual = clientProxy.query(query, credentials)
    assert(actual === expected)
  }

  def testQueryFails(credentials: RawCredentials, query: String, expectedErrorClass: Class[_]): Unit = {
    try {
      val actual = clientProxy.query(query, credentials)
      fail(s"Query should have failed. Instead succeeded with result:\n$actual")
    } catch {
      case ex: CompilationException =>
        val qe = ex.queryError
        logger.info(s"Query error: $qe")
        assert(ex.queryError.getClass === expectedErrorClass)
    }
  }

  def testQueryFailsWithSemanticError(credentials: RawCredentials, query: String): Unit = {
    testQueryFails(credentials, query, classOf[SemanticErrors])
  }

  def testQueryFailsWithParserError(credentials: RawCredentials, query: String): Unit = {
    testQueryFails(credentials, query, classOf[ParserError])
  }
}
