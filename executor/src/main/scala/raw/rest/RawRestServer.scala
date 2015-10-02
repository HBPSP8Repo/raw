package raw.rest

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopConf, ScallopOption}
import raw._
import raw.executor._
import raw.spark._
import raw.utils.RawUtils
import spray.can.Http.Bound
import spray.http.{MediaTypes, StatusCodes}
import spray.routing.{ExceptionHandler, SimpleRoutingApp}
import spray.util.LoggingContext

import scala.concurrent.Future

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

  case class SchemaRequest(module: String, token: String)

  case class RegisterFileRequest(protocol:String, url:String, filename:String, name:String, `type`:String, token:String)

  // Response sent when there is an error processing a query
  case class CompilationErrorResponse(errorType: String, error: QueryError)

  // Response sent when the handler code raises an exception
  case class ExceptionResponse(exceptionType: String, message: String, stackTrace: String)

}

/**
 * REST server exposing the following calls:
 *
 * - /register
 * - /query
 * - /schemas
 *
 * See the wiki on the github repo for details on the rest interface
 * @param executorArg scala or spark executor. Currently, on Scala executor is implemented.
 */
class RawRestServer(executorArg: String, storageDirCmdOption: Option[String]) extends SimpleRoutingApp with StrictLogging {

  import DefaultJsonMapper._
  import RawRestServer._

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

  final val port = 54321
  implicit val system = ActorSystem("simple-routing-app")

  private[this] def createJsonResponse(queryError: QueryError): String = {
    val response = new CompilationErrorResponse(queryError.getClass.getSimpleName, queryError)
    mapper.writeValueAsString(response)
  }

  private[this] def createJsonResponse(exception: Exception): String = {
    val sw = new StringWriter()
    exception.printStackTrace(new PrintWriter(sw))
    val response = new ExceptionResponse(exception.getClass.getName, exception.getMessage, sw.toString)
    mapper.writeValueAsString(response)
  }


  def exceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: ClientErrorException =>
        requestUri { uri =>
          logger.warn(s"Request to $uri failed", e)
          respondWithMediaType(MediaTypes.`application/json`) {
            complete(StatusCodes.BadRequest, createJsonResponse(e))
          }
        }
      case e: CompilationException =>
        requestUri { uri =>
          logger.warn(s"Request to $uri failed: ${e.queryError}")
          respondWithMediaType(MediaTypes.`application/json`) {
            val errorAsJson = createJsonResponse(e.queryError)
            e.queryError match {
              case se: SemanticErrors =>
                complete(StatusCodes.BadRequest, errorAsJson)
              case pe: ParserError =>
                complete(StatusCodes.BadRequest, errorAsJson)
              case ie: InternalError =>
                complete(StatusCodes.InternalServerError, errorAsJson)
              case _ => {
                logger.info(s"No match found for query error class: ${e.queryError}")
                complete(StatusCodes.InternalServerError, errorAsJson)
              }
            }
          }
        }
      // Generate an internal error response for any other unknown/unexpected exception.
      case e: Exception =>
        requestUri { uri =>
          logger.warn(s"Request to $uri failed.", e)
          respondWithMediaType(MediaTypes.`application/json`) {
            complete(StatusCodes.InternalServerError, createJsonResponse(e))
          }
        }
      // Any other exception will be handled by Spray and return a 500 status code.
    }

  def start(): Future[Bound] = {
    val queryPath = "query"
    val registerPath = "register"
    val schemasPath = "schemas"
    logger.info(s"Listening on localhost:$port/{$registerPath,$queryPath,$schemasPath}")
    val future: Future[Bound] = startServer("0.0.0.0", port = port) {
      handleExceptions(exceptionHandler) {
        (path(queryPath) & post) {
          headerValueByName("Raw-User") { rawUser =>
            headerValueByName("Raw-Query-Language") { queryLanguageString =>
              entity(as[String]) { query =>
                logger.info(s"Query request received. User: $rawUser, QueryLanguage: $queryLanguageString, Query:\n$query")
                val queryLanguage = QueryLanguages(queryLanguageString)
                val result = rawServer.doQuery(queryLanguage, query, rawUser)
                logger.info("Query succeeded. Returning result: " + result.take(30))
                respondWithMediaType(MediaTypes.`application/json`) {
                  complete(result)
                }
              }
            }
          }
        } ~
          /*
           dataDir must contain the following files:
           - <schemaName>.[csv|json]
           - schema.xml
           - properties.json
          */
          (path(registerPath) & post) {
            formFields("user", "schemaName", "dataDir") { (user, schemaName, dataDir) =>
              logger.info(s"$user, $schemaName, $dataDir")
              rawServer.registerSchema(schemaName, dataDir, user)
              respondWithMediaType(MediaTypes.`application/json`) {
                complete( """ {"registersuccess" = True } """)
              }
            }
          } ~
          (path("register-file") & post) {
            entity(as[String]) { body =>
              val request = mapper.readerFor(classOf[RegisterFileRequest]).readValue[RegisterFileRequest](body)
              logger.info(s"Register-file: $request")
              doRegisterFile(request)
              respondWithMediaType(MediaTypes.`application/json`) {
                complete( """ {"success" = True } """)
              }
            }
          } ~
          (path(schemasPath) & get) {
            headerValueByName("Raw-User") { user =>
              logger.info(s"Returning schemas for $user")
              val schemas: Seq[String] = rawServer.getSchemas(user)
              respondWithMediaType(MediaTypes.`application/json`) {
                complete(mapper.writeValueAsString(schemas))
              }
            }
          } ~
          (path(schemasPath) & post) {
            entity(as[String]) { body =>
              val request = mapper.readerFor(classOf[SchemaRequest]).readValue[SchemaRequest](body)
              val schemas: Seq[String] = doSchemas(request.module, request.token)
              respondWithMediaType(MediaTypes.`application/json`) {
                complete(mapper.writeValueAsString(Map("success" -> true, "schemas" -> schemas)))
              }
            }
          }
      }
    }
    future
  }

  def doSchemas(module: String, token: String): Seq[String] = {
    logger.info(s"Module: $module, token: $token")
    val rawUser = DropboxClient.getUserName(token)
    logger.info(s"Returning schemas for $rawUser")
    rawServer.getSchemas(rawUser)
  }

  def doRegisterFile(request: RegisterFileRequest) = {
    val localFile = RawUtils.getTemporaryDirectory().resolve(request.filename)
    DropboxClient.downloadFile(request.url, localFile)
  }

  def stop(): Unit = {
    logger.info("Shutting down")
    system.shutdown()
  }
}

object RawRestServerMain extends StrictLogging {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Scala/Spark OQL execution server")
      val executor: ScallopOption[String] = opt[String]("executor", default = Some("scala"), short = 'e')
      val storageDir: ScallopOption[String] = opt[String]("storage-dir", default = None, short = 's')
    }

    val restServer = new RawRestServer(Conf.executor(), Conf.storageDir.get)
    restServer.start()
  }
}
