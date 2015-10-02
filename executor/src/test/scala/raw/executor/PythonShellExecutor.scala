package raw.executor

import java.nio.file.{Paths, Path}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.impl.client.HttpClients
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.rest.RawRestServer
import raw.utils.RawUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process._

class PythonShellExecutor extends FunSuite with StrictLogging {


  test("Hello") {
    val inferrerPath = ConfigFactory.load().getString("raw.inferrer.path")
    val p = Paths.get(inferrerPath)
    logger.info(s"Path: $inferrerPath => $p")



  }
}
