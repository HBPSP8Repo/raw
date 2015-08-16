package raw.utils

import java.net.URI

import com.typesafe.scalalogging.StrictLogging

import scala.sys.process._

object DockerUtils extends StrictLogging {
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
    dockerCID = ("docker run -d --name=ldb -p 5001:5000 raw/ldb".!!).trim
    logger.info(s"Started container: $dockerCID")

    (1 to 10).foreach(i => {
      logger.info("Waiting for LDB server to start")
      val sb = new StringBuilder
      s"docker logs $dockerCID" ! ProcessLogger(sb.append(_), sb.append(_))
      val output = sb.toString()
      logger.info(s"Output: $output")
      if (output.contains("Running on")) {
        logger.info(s"LDB web server started")
        return
      }
      try {
        Thread.sleep(500)
      } catch {
        case ex:Exception => ex.printStackTrace()
      }
    }
    )
    try {
      stopDocker()
    } catch {
      case ex:Exception => // ignore
    }
    throw new RuntimeException("LDB failed to start.")
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
}
