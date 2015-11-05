package raw.executor

import java.io.IOException
import java.nio.file.Path
import java.util.Base64

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import raw.rest.DefaultJsonMapper
import raw.rest.RawService.{ExceptionResponse, QueryRequest, RegisterFileRequest}
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

  /**
   *
   * @param success
   * @param output Can be any json data type: String, array or object. Therefore it is declared as AnyRef so that the
   *               Jackson can deserialize it.
   * @param execution_time
   * @param compile_time
   */
  case class QueryResponse(success: String, output: AnyRef, execution_time: Long, compile_time: Long)

  val queryResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[QueryResponse])

  case class ParseErrorResponse(errorType: String, error: ParserError)

  val parseErrorResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[ParseErrorResponse])

  case class SemanticErrorsResponse(errorType: String, error: SemanticErrors)

  val semanticErrorsResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[SemanticErrorsResponse])

  val exceptionResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[ExceptionResponse])
}

class ClientErrorWrapperException(exceptionResponse: ExceptionResponse) extends Exception(exceptionResponse.toString)

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

  def query(query: String, credentials: RawCredentials): String = {
    val queryPost = new HttpPost("http://localhost:54321/query")
    credentials.configureRequest(queryPost)
    //    addBasicAuthHeader(queryPost, user, "doesnotmatter")
    val queryRequest = new QueryRequest(query)
    val reqBody = DefaultJsonMapper.mapper.writeValueAsString(queryRequest)
    queryPost.setEntity(new StringEntity(reqBody))
    val responseBody = executeRequest(queryPost)
    val response = queryResponseReader.readValue[QueryResponse](responseBody)
    // response.output is deserialized as AnyRef. This will be an Array, Map or String.
    // Convert it back to String for checking in the tests.
    DefaultJsonMapper.mapper.writeValueAsString(response.output)
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
    val filename = file.getFileName.toString
    val i = filename.lastIndexOf('.')
    val fileType = if (i > 0) {
      val extension = filename.substring(i + 1)
      logger.info(s"File type derived from extension: $extension.")
      extension
    } else {
      logger.info(s"No extension detected in file $file. Assuming text file.")
      "text"
    }
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
      val map = DefaultJsonMapper.mapper.readValue[Map[String,Object]](body, classOf[Map[String,Object]])
      logger.info(s"Result: $map")

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
