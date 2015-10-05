package raw.rest

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{DirectoryNotEmptyException, Files}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.Timeout
import raw._
import raw.executor.{CompilationException, PythonShellExecutor, RawServer}
import raw.rest.RawRestServer._
import spray.can.Http
import spray.http.HttpHeaders.{`Access-Control-Allow-Headers`, `Access-Control-Allow-Origin`, `Access-Control-Max-Age`}
import spray.http._

import scala.concurrent.duration._

class RawService(rawServer: RawServer) extends Actor with ActorLogging {

  import DefaultJsonMapper._
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
