package raw.executor

import java.io.IOException
import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import raw.rest.RawRestServer.{QueryRequest, RegisterFileRequest}
import raw.rest.{DefaultJsonMapper, RawRestServer}
import raw.{ParserError, SemanticErrors}

/* Classes representing the responses sent by the rest server and the corresponding Jackson deserializers */
object RawServiceClientProxy {

  case class SchemasResponse(success: String, schemas: List[String])

  val schemasResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[SchemasResponse])

  case class RegisterFileResponse(success: String, name: String)

  val registerFileResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[RegisterFileResponse])

  case class QueryResponse(success: String, output: String, execution_time: Long, compile_time: Long)

  val queryResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[QueryResponse])

  case class ParseErrorResponse(errorType: String, error: ParserError)

  val parseErrorResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[ParseErrorResponse])

  case class SemanticErrorsResponse(errorType: String, error: SemanticErrors)

  val sematicErrorsResponseReader = DefaultJsonMapper.mapper.readerFor(classOf[SemanticErrorsResponse])
}

class RawServiceClientProxy extends StrictLogging {

  import RawServiceClientProxy._

  val httpClient = HttpClients.createDefault()

  def query(query: String, user: String): String = {
    val queryPost = new HttpPost("http://localhost:54321/query")
    val queryRequest = new QueryRequest(query, user)
    val reqBody = DefaultJsonMapper.mapper.writeValueAsString(queryRequest)
    queryPost.setEntity(new StringEntity(reqBody))
    val responseBody = executeRequest(queryPost)
    val response = queryResponseReader.readValue[QueryResponse](responseBody)
    response.output
  }

  def registerFile(user: String, file: Path): String = {
    val filename = file.getFileName.toString
    val i = filename.lastIndexOf('.')
    assert(i > 0, s"Cannot recognize type of input file: $file.")
    val fileType = filename.substring(i + 1)
    val schema = filename.substring(0, i)
    val req = new RegisterFileRequest("file", file.toString, filename, schema, fileType, user)
    val registerPost = new HttpPost("http://localhost:54321/register-file")
    registerPost.setEntity(new StringEntity(DefaultJsonMapper.mapper.writeValueAsString(req)))
    val responseBody = executeRequest(registerPost)
    val response = registerFileResponseReader.readValue[RegisterFileResponse](responseBody)
    response.name
  }

  def getSchemas(user: String): Set[String] = {
    val schemasPost = new HttpPost("http://localhost:54321/schemas")
    val req = new RawRestServer.SchemaRequest("unnused", user)
    val reqBody = DefaultJsonMapper.mapper.writeValueAsString(req)
    schemasPost.setEntity(new StringEntity(reqBody))
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
      val qe = map("errorType") match {
        case "SemanticErrors" => sematicErrorsResponseReader.readValue[SemanticErrorsResponse](body).error
        case "ParserError" => parseErrorResponseReader.readValue[ParseErrorResponse](body).error
        case a@_ => throw new AssertionError("Unknown error type: " + a)
      }
      throw new CompilationException(qe)
    } else {
      throw new IOException("Unexpected status code. Was expecting 2XX or 4XX. Found: " + response.getStatusLine + "\n" + body)
    }
  }
}
