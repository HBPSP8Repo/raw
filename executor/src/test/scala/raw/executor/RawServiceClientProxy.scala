package raw.executor

import java.io.IOException
import java.nio.file.Path
import java.util.Base64

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import raw.rest.RawServiceActor._
import raw.utils.{DefaultJsonMapper, FileTypes}
import raw.{ParserError, SemanticErrors}


object DropboxAuthUsers {
  final val TestUserJoe = new DropboxCredentials("123456789-Joe_Test_Doe")
  final val TestUserJane = new DropboxCredentials("987654321-Jane_Test_Dane")
  final val TestUserNonexisting = new DropboxCredentials("000000-Elvis")
}

object BasicAuthUsers {
  final val TestUserJoe = new BasicCredentials("123456789-Joe_Test_Doe", "1234")
  final val TestUserJane = new BasicCredentials("987654321-Jane_Test_Dane", "1234")
  final val TestUserNonexisting = new BasicCredentials("000000-Elvis", "1234")
}


/* Classes representing the responses sent by the rest server and the corresponding Jackson deserializers */
object RawServiceClientProxy {

  case class SchemasResponse(success: String, schemas: List[String])

  val schemasResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[SchemasResponse])

  case class RegisterFileResponse(success: String, name: String)

  val registerFileResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[RegisterFileResponse])

  val queryResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[QueryResponse])
  val queryBlockResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[QueryBlockResponse])

  case class ParseErrorResponse(errorType: String, error: ParserError)

  val parseErrorResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[ParseErrorResponse])

  case class SemanticErrorsResponse(errorType: String, error: SemanticErrors)

  val semanticErrorsResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[SemanticErrorsResponse])

  val exceptionResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[ExceptionResponse])
}

class ClientErrorWrapperException(val exceptionResponse: ExceptionResponse) extends Exception(exceptionResponse.toString)

trait RawCredentials {
  def configureRequest(request: HttpUriRequest): Unit
}

case class DropboxCredentials(token: String) extends RawCredentials {
  override def configureRequest(request: HttpUriRequest): Unit = {
    request.setHeader("Authorization", "Bearer " + token)
  }
}

case class BasicCredentials(user: String, pass: String) extends RawCredentials {
  override def configureRequest(request: HttpUriRequest): Unit = {
    val encoding = Base64.getEncoder.encodeToString(s"$user:$pass".getBytes())
    request.setHeader("Authorization", "Basic " + encoding)
  }
}


class RawServiceClientProxy extends StrictLogging {

  import RawServiceClientProxy._

  val httpClient = HttpClients.createDefault()

  def doQuery(url: String, payload: AnyRef, credentials: RawCredentials): String = {
    val queryPost = new HttpPost(url)
    credentials.configureRequest(queryPost)
    val reqBody = DefaultJsonMapper.mapper.writeValueAsString(payload)
    queryPost.setEntity(new StringEntity(reqBody))
    executeRequest(queryPost)
  }

  def query(query: String, credentials: RawCredentials): String = {
    val queryRequest = new QueryRequest(query)
    val responseBody = doQuery("http://localhost:54321/query", queryRequest, credentials)
    val response = queryResponseReader.readValue[QueryResponse](responseBody)
    // response.output is deserialized as AnyRef. This will be an Array, Map or String.
    // Convert it back to String for checking in the tests.
    DefaultJsonMapper.mapper.writeValueAsString(response.output)
  }

  def queryStart(query: String, resultsPerPage: Int, credentials: RawCredentials): QueryBlockResponse = {
    val queryRequest = new QueryStartRequest(query, resultsPerPage)
    val responseBody = doQuery("http://localhost:54321/query-start", queryRequest, credentials)
    queryBlockResponseReader.readValue[QueryBlockResponse](responseBody)
  }

  def queryNext(token: String, resultsPerPage: Int, credentials: RawCredentials): QueryBlockResponse = {
    val queryRequest = new QueryNextRequest(token, resultsPerPage)
    val responseBody = doQuery("http://localhost:54321/query-next", queryRequest, credentials)
    queryBlockResponseReader.readValue[QueryBlockResponse](responseBody)
  }

  def queryClose(token: String, credentials: RawCredentials): Unit = {
    val queryRequest = new QueryCloseRequest(token)
    val responseBody: String = doQuery("http://localhost:54321/query-close", queryRequest, credentials)
    logger.info(s"Response: $responseBody")
  }

  def registerLocalFile(credentials: RawCredentials, file: Path): String = {
    val filename = file.getFileName.toString
    val i = filename.lastIndexOf('.')
    assert(i > 0, s"Cannot recognize type of input file: $file.")
    val fileType = filename.substring(i + 1)
    val schema = filename.substring(0, i)
    registerFile("file", file.toString, filename, schema, fileType, credentials)
  }

  def registerLocalFile(credentials: RawCredentials, file: Path, schema: String): String = {
    logger.info(s"Registering file: $file, $schema")
    val filename = file.getFileName.toString
    val fileType = FileTypes.inferFileType(filename)
    registerFile("file", file.toString, filename, schema, fileType, credentials)
  }

  def registerFile(protocol: String, url: String, filename: String, name: String, `type`: String, credentials: RawCredentials): String = {
    val req = new RegisterFileRequest(protocol, url, filename, name, `type`)
    val registerPost = new HttpPost("http://localhost:54321/register-file")
    credentials.configureRequest(registerPost)
    //    addBasicAuthHeader(registerPost, user, "doesnotmatter")
    registerPost.setEntity(new StringEntity(DefaultJsonMapper.mapper.writeValueAsString(req)))
    val responseBody = executeRequest(registerPost)
    val response = registerFileResponseReader.readValue[RegisterFileResponse](responseBody)
    response.name
  }

  def getSchemas(credentials: RawCredentials): Set[String] = {
    val schemasPost = new HttpGet("http://localhost:54321/schemas")
    //    addBasicAuthHeader(schemasPost, user, "doesnotmatter")
    credentials.configureRequest(schemasPost)
    val responseBody = executeRequest(schemasPost)
    val response = schemasResponseReader.readValue[SchemasResponse](responseBody)
    val asSet = response.schemas.toSet
    assert(asSet.size == response.schemas.size, "Response contains duplicated elements: " + response.schemas)
    asSet
  }


  private[this] def executeRequest(request: HttpUriRequest): String = {
    logger.info("Sending request: " + request)
    val response = httpClient.execute(request)
    val body = IOUtils.toString(response.getEntity.getContent)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode < 300) {
      body
    } else if (statusCode >= 400 && statusCode < 500) {
      // Deserialize as generic map to obtain the class of the error object, so that after we can deserialize
      // it as an instance of that class. We need to provide to Jackson the exact subtype of QueryError, providing
      // the base abstract class is not enough
      val map = DefaultJsonMapper.mapper.readValue[Map[String, Object]](body, classOf[Map[String, Object]])
      logger.debug(s"Result: $map")

      // A client error status may indicate:
      // 1. Malformed request: invalid URL on register-request, non existing file, bad file
      // 2. syntax error in the query
      // 3. Semantic error in the query.
      // 4. Another error attributable to the client
      // For 1 and 4, the server sends back a response describing a generic exception with the details of the problem
      // For 2 and 3, it sends back a CompilationException, with specific information on the error parser or semantic error.

      // Handle cases 1 and 4
      if (map.contains("exceptionType")) {
        val exResponse = exceptionResponseReader.readValue[ExceptionResponse](body)
        throw new ClientErrorWrapperException(exResponse)
      }
      // Handle cases 2 and 3
      val qe = map("errorType") match {
        case "SemanticErrors" => semanticErrorsResponseReader.readValue[SemanticErrorsResponse](body).error
        case "ParserError" => parseErrorResponseReader.readValue[ParseErrorResponse](body).error
        case a@_ => throw new AssertionError("Unknown error type: " + a)
      }
      logger.info("Query error: " + qe)
      throw new CompilationException(qe.toString, qe)
    } else {
      throw new IOException("Unexpected status code. Was expecting 2XX or 4XX. Found: " + response.getStatusLine + "\n" + body)
    }
  }
}
