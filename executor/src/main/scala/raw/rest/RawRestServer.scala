package raw.rest

import java.nio.file.{Path, Paths}

import akka.actor.{ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.routing.RoundRobinPool
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw._
import raw.executor._
import raw.rest.RawRestServer._
import raw.spark._
import spray.can.Http
import spray.can.Http.Bound

import scala.concurrent.Future
import scala.concurrent.duration._


/* Object mapper used to read/write any JSON received/sent by the rest server */
object DefaultJsonMapper extends StrictLogging {
  val mapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    //    om.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    om.setSerializationInclusion(Include.ALWAYS)
    om
  }
}


/* Generic exception, which the request handler code can raise to send a 400 response to the client. 
* This exception will be caught by the exception handler below and transformed in a 400 response*/
class ClientErrorException(msg: String) extends Exception(msg)

object RawRestServer {

  import DefaultJsonMapper._

  final val port = 54321

  case class QueryRequest(query: String, token: String)

  val queryRequestReader = mapper.readerFor(classOf[QueryRequest])

  case class SchemaRequest(module: String, token: String)

  val schemaRequestReader = mapper.readerFor(classOf[SchemaRequest])

  case class RegisterFileRequest(protocol: String, url: String, filename: String, name: String, `type`: String, token: String)

  val registerRequestReader = mapper.readerFor(classOf[RegisterFileRequest])

  // Response sent when there is an error processing a query
  case class CompilationErrorResponse(errorType: String, error: QueryError)

  // Response sent when the handler code raises an exception
  case class ExceptionResponse(exceptionType: String, message: String, stackTrace: String)

}


class RawRestServer(executorArg: String, storageDirCmdOption: Option[String]) extends StrictLogging {

  import akka.pattern.ask

  val rawServer = {
    val storageDir: Path = storageDirCmdOption match {
      case None =>
        try {
          Paths.get(ConfigFactory.load().getString("raw.datadir"))
        } catch {
          case ex: ConfigException.Missing => StorageManager.defaultStorageDir
        }
      case Some(dir) => Paths.get(dir)
    }
    new RawServer(storageDir)
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
    val handler = system.actorOf(
      RoundRobinPool(5).props(Props {
        new RawService(rawServer)
      }),
      name = "handler")

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

