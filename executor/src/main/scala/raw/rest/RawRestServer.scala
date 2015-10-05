package raw.rest

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{DirectoryNotEmptyException, Files, Path, Paths}

import akka.actor.{ActorSystem, Props, _}
import akka.io.{IO, Tcp}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopConf, ScallopOption}
import raw._
import raw.executor._
import raw.rest.DefaultJsonMapper._
import raw.rest.RawRestServer._
import raw.rest.RawRestServer._
import raw.spark._
import spray.can.Http
import spray.can.Http.Bound
import spray.http.HttpHeaders.{`Access-Control-Allow-Headers`, `Access-Control-Allow-Origin`, `Access-Control-Max-Age`}
import spray.http._

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


  //  POST /query HTTP/1.1
  //  Host: localhost:54321
  //  Connection: keep-alive
  //  Content-Length: 94
  //  Origin: http://localhost:5000
  //  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36
  //  Accept: */*
  //  Referer: http://localhost:5000/static/index.html
  //  Accept-Encoding: gzip, deflate
  //  Accept-Language: en-US,en;q=0.8,de;q=0.6,pt;q=0.4
  //
  //  {"query":"array2d","token":"597g_VAPJEEAAAAAAAAl2XT8b48WXOFgMPVyJu1lDlKy7cSfXZKhlfxIU_cUWWTo"}

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

class RawRestService(rawServer: RawServer) extends Actor with ActorLogging {

  import HttpMethods._

  implicit val timeout: Timeout = 1.second
  // for the actor 'asks' // ExecutionContext for the futures and scheduler
  val queryPath = "/query"
  val registerPath = "/register-file"
  val schemasPath = "/schemas"

  val corsHeaders = List(
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(1728000),
    `Access-Control-Allow-Origin`(AllOrigins))

  def withCORS(response: HttpResponse): HttpResponse = {
    response.withHeaders(response.headers ++ corsHeaders)
  }

  def complete(sender: ActorRef, response: HttpResponse): Unit = {
    sender ! withCORS(response)
  }

  def processRequest(httpRequest: HttpRequest, f: (HttpRequest) => HttpResponse): HttpResponse = {
    val uri = httpRequest.uri
    try {
      f(httpRequest)
    } catch {
      case e: ClientErrorException =>
        log.warning(s"Request to $uri failed", e)
        HttpResponse(StatusCodes.BadRequest, HttpEntity(ContentTypes.`application/json`, createJsonResponse(e)))

      case e: CompilationException =>
        log.warning(s"Request to $uri failed: ${e.queryError}")
        val errorAsJson = createJsonResponse(e.queryError)
        val statusCode = e.queryError match {
          case se: SemanticErrors => StatusCodes.BadRequest
          case pe: ParserError => StatusCodes.BadRequest
          case ie: InternalError => StatusCodes.InternalServerError
          case _ => {
            log.warning(s"No match found for query error class: ${e.queryError}")
            StatusCodes.InternalServerError
          }
        }
        HttpResponse(statusCode, HttpEntity(ContentTypes.`application/json`, errorAsJson))

      // Generate an internal error response for any other unknown/unexpected exception.
      case e: Throwable =>
        log.warning(s"Request to $uri failed.", e)
        HttpResponse(StatusCodes.InternalServerError, HttpEntity(ContentTypes.`application/json`, createJsonResponse(e)))
    }
  }

  override def receive: Receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case Timedout(HttpRequest(_, Uri.Path("/timeout/timeout"), _, _, _)) =>
      log.info("Dropping Timeout message")

    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      sender ! HttpResponse(
        status = 500,
        entity = "The " + method + " request to '" + uri + "' has timed out..."
      )

    case r@HttpRequest(POST, Uri.Path("/query"), _, _, _) =>
      complete(sender, processRequest(r, doQuery))

    case r@HttpRequest(POST, Uri.Path("/register-file"), _, _, _) =>
      complete(sender, processRequest(r, doRegisterFile))

    case r@HttpRequest(POST, Uri.Path("/schemas"), _, _, _) =>
      complete(sender, processRequest(r, doSchemas))

    case r@_ =>
      log.warning("Unknown request: " + r)
      complete(sender, HttpResponse(StatusCodes.BadRequest, HttpEntity(s"Unknown request: $r")))
  }


  private[this] def createJsonResponse(queryError: QueryError): String = {
    val response = new CompilationErrorResponse(queryError.getClass.getSimpleName, queryError)
    mapper.writeValueAsString(response)
  }

  private[this] def createJsonResponse(exception: Throwable): String = {
    val sw = new StringWriter()
    exception.printStackTrace(new PrintWriter(sw))
    val response = new ExceptionResponse(exception.getClass.getName, exception.getMessage, sw.toString)
    mapper.writeValueAsString(response)
  }

  private[this] def doQuery(httpRequest: HttpRequest): HttpResponse = {
    val request = queryRequestReader.readValue[QueryRequest](httpRequest.entity.asString)
    log.info(s"Query request: $request")
    // TODO: Send query language in request
    val queryLanguage = QueryLanguages("qrawl")
    val rawUser = DropboxClient.getUserName(request.token)
    val query = request.query
    val result = rawServer.doQuery(queryLanguage, query, rawUser)
    val response = Map("success" -> true, "output" -> result, "execution_time" -> 0, "compile_time" -> 0)
    val serializedResponse = mapper.writeValueAsString(response)
    log.info("Query succeeded. Returning result: " + serializedResponse.take(100))
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, serializedResponse))
  }

  private[this] def doSchemas(httpRequest: HttpRequest): HttpResponse = {
    val request = schemaRequestReader.readValue[SchemaRequest](httpRequest.entity.asString)
    log.info(s"Module: ${request.module}, token: ${request.token}")
    val rawUser = DropboxClient.getUserName(request.token)
    log.info(s"Returning schemas for $rawUser")
    val schemas: Seq[String] = rawServer.getSchemas(rawUser)
    val response = Map("success" -> true, "schemas" -> schemas)
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(response)))
  }

  private[this] def doRegisterFile(httpRequest: HttpRequest): HttpResponse = {
    val request = registerRequestReader.readValue[RegisterFileRequest](httpRequest.entity.asString)
    log.info(s"doRegisterFile: $request")
    val stagingDirectory = Files.createTempDirectory("raw-stage")
    try {
      val localFile = stagingDirectory.resolve(request.name + "." + request.`type`)
      DropboxClient.downloadFile(request.url, localFile)
      PythonShellExecutor.inferSchema(localFile, request.`type`, request.name)
      // Register the schema
      rawServer.registerSchema(request.name, stagingDirectory, DropboxClient.getUserName(request.token))
      val response = Map("success" -> true, "name" -> request.name)
      HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(response)))
    } finally {
      try {
        Files.deleteIfExists(stagingDirectory)
      } catch {
        case ex: DirectoryNotEmptyException => log.warning("Could not delete directory", ex)
      }
    }
  }
}


class RawRestServer(executorArg: String, storageDirCmdOption: Option[String]) extends StrictLogging {

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

  import akka.pattern.ask

  def start(): Future[Bound] = {
    val handler = system.actorOf(
      RoundRobinPool(5).props(Props {
        new RawRestService(rawServer)
      }),
      name = "handler")
    IO(Http).ask(Http.Bind(handler, interface = "0.0.0.0", port = 54321))(1.second)
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

object RawRestServerMain2 extends StrictLogging {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Scala/Spark OQL execution server")
      val executor: ScallopOption[String] = opt[String]("executor", default = Some("scala"), short = 'e')
      val storageDir: ScallopOption[String] = opt[String]("storage-dir", default = None, short = 's')
    }

    val server = new RawRestServer(Conf.executor(), Conf.storageDir.get)
    server.start()
  }
}
