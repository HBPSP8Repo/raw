package raw

import java.net.URI

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.sys.process._

trait LDBDockerContainer extends BeforeAndAfterAll with StrictLogging {
  self: Suite =>

  private[this] var dockerCID: String = _

  def setEnvironment() {
    val dockerAddress = System.getenv().get("DOCKER_HOST")
    val ldbServerAddress = if (dockerAddress == null) {
      println("WARN: No DOCKER_HOST environment variable found. Using default of localhost for LDB compilation server")
      "http://localhost:5001/raw-plan"
    } else {
      println("Docker host: " + dockerAddress)
      val uri = new URI(dockerAddress)
      s"http://${uri.getHost}:5001/raw-plan"
    }
    println(s"RAW compilation server at $ldbServerAddress")
    System.setProperty("raw.compile.server.host", ldbServerAddress)
  }


  def startDocker(): Unit = {
    println("Starting docker")
    val cID = ("docker run -d -p 5001:5000 raw/ldb".!!).trim
    println(s"Started container: $cID")
    dockerCID = cID
  }

  def stopDocker(): Unit = {
    println(s"Stopping docker container: $dockerCID")
    val separator = "== Docker container logs =="
    println(separator)
    s"docker stop -t 0 ${dockerCID}".!!
    s"docker logs ${dockerCID}".!
    s"docker rm $dockerCID".!!
    println(separator)
  }

  override def beforeAll() {
    super.beforeAll()
    setEnvironment()
    startDocker()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopDocker()
  }
}
