package raw.rest

import java.nio.file.{Path, Paths}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.routing.{FromConfig, RoundRobinPool}
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.executor._
import raw.rest.RawRestServer._
import raw.spark._
import raw.storage.{LocalStorageBackend, StorageBackend, StorageManager}
import spray.can.Http
import spray.can.Http.Bound

import scala.concurrent.Future
import scala.concurrent.duration._


object RawRestServer {
  final val port = 54321
}


class RawRestServer(executorArg: String, storageDirCmdOption: Option[String]) extends StrictLogging {
  // Mixin to allow testing with a mock implementation
  dropboxClient: DropboxClient =>

  import akka.pattern.ask

  val rawServer = {
    val storageDir: Path = storageDirCmdOption match {
      case None =>
        try {
          Paths.get(ConfigFactory.load().getString("raw.storage.datadir"))
        } catch {
          case ex: ConfigException.Missing => StorageManager.defaultDataDir
        }
      case Some(dir) => Paths.get(dir)
    }

    val storageBackend =
      try {
        StorageBackend(ConfigFactory.load().getString("raw.storage.backend"))
      } catch {
        case ex: ConfigException.Missing => {
          logger.info("No value for property \"raw.storage.backend\". Using local storage")
          LocalStorageBackend
        }
      }

    new RawServer(storageDir, storageBackend)
  }

  val sc: Option[SparkContext] = executorArg match {
    case "scala" => logger.info("Using Scala-only executor"); None
    case "spark" =>
      logger.info("Using Spark")
      lazy val sc: SparkContext = {
        Thread.currentThread().setContextClassLoader(CodeGenerator.rawClassloader)
        logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
        new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
      }
      Some(sc)
    case exec@_ =>
      throw new IllegalArgumentException(s"Invalid executor: $exec. Valid options: [scala, spark]")
  }

  implicit val system = ActorSystem()

  def start(): Future[Bound] = {
    // Get the configuration from the config files (reference.conf or application.conf)
    val props = FromConfig.props(Props {
      new RawServiceActor(rawServer, dropboxClient)
    })
    val handler: ActorRef = system.actorOf(props, "rest-server-handler")
    logger.info(s"Created actor: ${handler}")

    IO(Http).ask(Http.Bind(handler, interface = "0.0.0.0", port = port))(1.second)
      .flatMap {
        case b: Http.Bound ⇒ Future.successful(b)
        case Tcp.CommandFailed(b: Http.Bind) => Future.failed(new RuntimeException(
          "Binding failed. Switch on DEBUG-level logging for `akka.io.TcpListener` to log the cause."))
      }(system.dispatcher)
  }

  def stop(): Unit = {
    logger.info("Shutting down")
    system.shutdown()
  }
}
