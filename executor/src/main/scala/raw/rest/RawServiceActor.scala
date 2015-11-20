package raw.rest

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import com.google.common.base.Stopwatch
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import raw._
import raw.executor._
import raw.utils.DefaultJsonMapper
import spray.can.Http
import spray.http.HttpHeaders.{Authorization, `Access-Control-Allow-Headers`, `Access-Control-Allow-Origin`, `Access-Control-Max-Age`}
import spray.http.StatusCodes.ClientError
import spray.http._

import scala.concurrent.duration._


/* Generic exception, which the request handler code can raise to send a 400 response to the client.
* This exception will be caught by the exception handler below and transformed in a 400 response*/
class ClientErrorException(msg: String, cause: Throwable, val statusCode: ClientError = StatusCodes.BadRequest) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)

  def this(cause: Throwable) = this(cause.getMessage, cause)
}

object RawServiceActor extends StrictLogging {

  import DefaultJsonMapper._

  // Scala representation of the JSON requests. Used by Jackson to convert between scala object and JSON
  case class QueryRequest(query: String)

  case class QueryStartRequest(query: String, resultsPerPage: Int)

  case class QueryNextRequest(token: String, resultsPerPage: Int)

  case class QueryCloseRequest(token: String)

  /**
    * @param output Can be any json data type: String, array or object. Therefore it is declared as AnyRef so that the
    *               Jackson can deserialize it.
    */
  case class QueryResponse(output: Any, compilationTime: Long, executionTime: Long)

  case class QueryBlockResponse(data: Any, start: Int, size: Int, hasMore: Boolean, token: String, var compilationTime: Long = -1, var executionTime: Long = -1)

  case class SchemasResponse(schemas: Seq[String])

  case class RegisterFileRequest(protocol: String, url: String, filename: String, name: String, `type`: String)

  // Response sent when there is an error processing a query
  case class CompilationErrorResponse(errorType: String, error: QueryError)

  // Response sent when the handler code raises an exception
  case class ExceptionResponse(exceptionType: String, message: String, stackTrace: String)

  // Serializers
  val queryResponseWriter = mapper.writerFor(classOf[QueryResponse])
  val queryBlockResponseWriter = mapper.writerFor(classOf[QueryBlockResponse])

  // Deserializers
  val queryRequestReader = mapper.readerFor(classOf[QueryRequest])
  val queryStartRequestReader = mapper.readerFor(classOf[QueryStartRequest])
  val queryNextRequestReader = mapper.readerFor(classOf[QueryNextRequest])
  val queryCloseRequestReader = mapper.readerFor(classOf[QueryCloseRequest])
  val registerRequestReader = mapper.readerFor(classOf[RegisterFileRequest])

  // If authentication is disabled, use this as default user name
  final val DEMO_USER = "demo"

  val demoMode: Boolean = {
    val key = "raw.demo-mode"
    val configValue = ConfigFactory.load().getString(key)
    val enabled = configValue == "enabled"
    logger.info(s"Demo mode: $enabled. Derived from config property: $key = $configValue.")
    enabled
  }

  // for the actor 'asks' // ExecutionContext for the futures and scheduler
  implicit val timeout: Timeout = 1.second

  val corsHeaders = List(
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(1728000),
    `Access-Control-Allow-Origin`(AllOrigins))

  val queryCache = new QueryCache()
}

class RawServiceActor(rawServer: RawServer, dropboxClient: DropboxClient) extends Actor with StrictLogging {

  import DefaultJsonMapper._
  import HttpMethods._
  import RawServiceActor._

  def withCORS(response: HttpResponse): HttpResponse = {
    response.withHeaders(response.headers ++ corsHeaders)
  }

  def complete(sender: ActorRef, response: HttpResponse): Unit = {
    sender ! withCORS(response)
  }

  def processRequest(httpRequest: HttpRequest, f: (HttpRequest) => HttpResponse): HttpResponse = {
    val uri = httpRequest.uri
    logger.info(s"Received request: $httpRequest")
    try {
      val httpResponse = f(httpRequest)
      val responseAsString = s"HttpResponse(${httpResponse.status}, ${httpResponse.headers}, ${httpResponse.entity})"
      val str = responseAsString.take(200) + {
        if (responseAsString.length > 200) "..." else ""
      }
      logger.info(str)
      httpResponse
    } catch {
      case e: ClientErrorException =>
        logger.warn(s"Request to $uri failed", e)
        HttpResponse(e.statusCode, HttpEntity(ContentTypes.`application/json`, createJsonResponse(e)))

      case e: CompilationException =>
        logger.warn(s"Request to $uri failed: ${e.queryError}")
        val errorAsJson = createJsonResponse(e.queryError)
        val statusCode = e.queryError match {
          case se: SemanticErrors => StatusCodes.BadRequest
          case pe: ParserError => StatusCodes.BadRequest
          case ie: InternalError => StatusCodes.InternalServerError
          case _ => {
            logger.warn(s"No match found for query error class: ${e.queryError}")
            StatusCodes.InternalServerError
          }
        }
        HttpResponse(statusCode, HttpEntity(ContentTypes.`application/json`, errorAsJson))

      // Generate an internal error response for any other unknown/unexpected exception.
      case e: Throwable =>
        logger.warn(s"Request to $uri failed.", e)
        HttpResponse(StatusCodes.InternalServerError, HttpEntity(ContentTypes.`application/json`, createJsonResponse(e)))
    }
  }

  override def receive: Receive = {
    // TODO: What other lifecycle commands can this actor receive? ConfirmedClose
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case Timedout(HttpRequest(_, Uri.Path("/timeout/timeout"), _, _, _)) =>
      logger.info("Dropping Timeout message")

    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      sender ! HttpResponse(
        status = 500,
        entity = "The " + method + " request to '" + uri + "' has timed out..."
      )

    case r@HttpRequest(POST, Uri.Path("/query"), _, _, _) =>
      complete(sender, processRequest(r, doQuery))

    // Pagination
    case r@HttpRequest(POST, Uri.Path("/query-start"), _, _, _) =>
      complete(sender, processRequest(r, doQueryStart))

    case r@HttpRequest(POST, Uri.Path("/query-next"), _, _, _) =>
      complete(sender, processRequest(r, doQueryNext))

    case r@HttpRequest(POST, Uri.Path("/query-close"), _, _, _) =>
      complete(sender, processRequest(r, doQueryClose))

    case r@HttpRequest(POST, Uri.Path("/register-file"), _, _, _) =>
      complete(sender, processRequest(r, doRegisterFile))

    case r@HttpRequest(GET, Uri.Path("/schemas"), _, _, _) =>
      complete(sender, processRequest(r, doSchemas))

    case r@HttpRequest(_, _, _, _, _) =>
      logger.warn(s"Unknown request: $r")
      complete(sender, HttpResponse(StatusCodes.BadRequest, HttpEntity(s"Unknown request: $r")))

    case r@_ =>
      logger.debug("Ignoring command: " + r)
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

  private[this] def getUser(httpRequest: HttpRequest): String = {
    if (demoMode) {
      // In demo mode, the server does not except the Authorization header on the request, the UI should not send it.
      // So raise a warning if it is found, as this may indicate a misconfiguration.
      val auth: Option[Authorization] = httpRequest.header[HttpHeaders.Authorization]
      if (!auth.isEmpty) {
        logger.warn("Request contains Authorization header but server is in demo mode. Is client misconfigured? Ignoring header and proceeding.")
      }
      return DEMO_USER
    } else {
      val auth: Option[Authorization] = httpRequest.header[HttpHeaders.Authorization]
      auth match {
        case None => //TODO: raise a 401.
          throw new ClientErrorException("Request not authenticated, no Authorization header found.", null, StatusCodes.Unauthorized)

        case Some(auth) => auth.credentials match {
          case BasicHttpCredentials(user, pass) =>
            logger.info(s"Basic auth detected. Using user from header: $user")
            user

          case auth@OAuth2BearerToken(token) =>
            val user = dropboxClient.getUserName(token)
            logger.info( s"""OAuth2 auth detected: "$auth". User: $user""")
            user

          case auth@GenericHttpCredentials(scheme, token, params) =>
            logger.info(s"GenericHttpCredentials: $auth")
            throw new ClientErrorException(s"Unsupported authentication mechanism: $auth", null, StatusCodes.Unauthorized)
        }
      }
    }
  }

  private[this] def doQuery(httpRequest: HttpRequest): HttpResponse = {
    val request = queryRequestReader.readValue[QueryRequest](httpRequest.entity.asString)
    val rawUser = getUser(httpRequest)
    val query = request.query
    // We only support qrawl, so the first argument can be refactored out. Requires also to cleanup the
    // RawServer and RawCompiler
    val compiledQuery = rawServer.doQuery(QueryLanguages("qrawl"), query, rawUser)
    val result = compiledQuery.computeResult
    val response = QueryResponse(result, compiledQuery.compilationTime, compiledQuery.executionTime)
    val serializedResponse = queryResponseWriter.writeValueAsString(response)
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, serializedResponse))
  }

  private[this] def doQueryStart(httpRequest: HttpRequest): HttpResponse = {
    val request = queryStartRequestReader.readValue[QueryStartRequest](httpRequest.entity.asString)
    val rawUser = getUser(httpRequest)
    val query = request.query
    val rawQuery: RawQuery = rawServer.doQueryStart(QueryLanguages("qrawl"), query, rawUser)

    val startTime = Stopwatch.createStarted()
    val openQuery = queryCache.newOpenQuery(query, rawQuery)
    val response: QueryBlockResponse = if (!openQuery.hasNext) {
      // No results.
      QueryBlockResponse(null, 0, 0, false, null)
    } else {
      queryCache.put(openQuery)
      nextQueryBlock(openQuery.token, request.resultsPerPage)
    }
    val executionTime = startTime.elapsed(TimeUnit.MILLISECONDS)
    response.compilationTime = rawQuery.compilationTime
    response.executionTime = executionTime
    logger.info(s"Execution time, doQueryStart: $executionTime ms")
    val serializedResponse = queryBlockResponseWriter.writeValueAsString(response)
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, serializedResponse))
  }

  private[this] def doQueryNext(httpRequest: HttpRequest): HttpResponse = {
    val request = queryNextRequestReader.readValue[QueryNextRequest](httpRequest.entity.asString)
    val startTime = Stopwatch.createStarted()
    val response = nextQueryBlock(request.token, request.resultsPerPage)
    val executionTime = startTime.elapsed(TimeUnit.MILLISECONDS)
    response.executionTime = executionTime
    logger.info(s"Execution time, doQueryNext: $executionTime ms")
    val serializedResponse = queryBlockResponseWriter.writeValueAsString(response)
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, serializedResponse))
  }

  private[this] def doQueryClose(httpRequest: HttpRequest): HttpResponse = {
    val request = queryCloseRequestReader.readValue[QueryCloseRequest](httpRequest.entity.asString)
    queryCache.close(request.token)
    HttpResponse()
  }

  private[this] def nextQueryBlock(token: String, resultsPerPage: Int): QueryBlockResponse = {
    queryCache.get(token) match {
      case Some(openQuery) =>
        val start = openQuery.position
        // If the json request does not contain a resultsPerPage field (the field is set to 0 by Jackson),
        // we use the default value.
        val nextBlock = if (resultsPerPage == 0) {
          openQuery.next()
        } else {
          openQuery.next(resultsPerPage)
        }
        val hasNext = openQuery.hasNext
        // Do not send token if there are no more results
        val nextToken = if (hasNext) token else null
        QueryBlockResponse(nextBlock, start, nextBlock.size, hasNext, nextToken)

      case None => throw new ClientErrorException(s"Cannot find a query for token $token")
    }
  }

  private[this] def doSchemas(httpRequest: HttpRequest): HttpResponse = {
    val rawUser = getUser(httpRequest)
    val schemas: Seq[String] = rawServer.getSchemas(rawUser)
    val response = mapper.writeValueAsString(SchemasResponse(schemas))
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, response))
  }

  private[this] def doRegisterFile(httpRequest: HttpRequest): HttpResponse = {
    val request = registerRequestReader.readValue[RegisterFileRequest](httpRequest.entity.asString)
    val rawUser = getUser(httpRequest)
    val stagingDirectory = Files.createTempDirectory(rawServer.storageManager.stageDirectory, "raw-stage")
    val localFile = stagingDirectory.resolve(request.name + "." + request.`type`)
    dropboxClient.downloadFile(request.url, localFile)
    InferrerShellExecutor.inferSchema(localFile, request.`type`)
    // Register the schema
    rawServer.registerSchema(request.name, stagingDirectory, rawUser)
    val response = Map("name" -> request.name)
    // Do not delete the staging directory if there is a failure (exception). Leave it for debugging
    try {
      FileUtils.deleteDirectory(stagingDirectory.toFile)
    } catch {
      case ex: Exception => logger.warn("Could not delete temporary directory. Expected an empty directory.", ex)
    }
    HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(response)))
  }
}
