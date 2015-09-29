package raw.rest

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopConf, ScallopOption}
import raw.QueryLanguages
import raw.executor._
import raw.spark._
import spray.can.Http.Bound
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing.{ExceptionHandler, SimpleRoutingApp}
import spray.util.LoggingContext

import scala.concurrent.Future

case class RegisterRequest(schemaName: String, schemaDefXml: String, filePath: String)

object RegisterRequestJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val PortofolioFormats = jsonFormat3(RegisterRequest)
}

/**
 * REST server exposing the following calls:
 *
 * - /register
 * - /query
 *
 * A register request should contain the schema as XML in the body and have the following headers
 * {{{
   Content-Type: application/xml
   Raw-Schema-Name: <schemaName>
   Raw-File: <http[s]: or file: uri>

   BODY: <Schema in XML format.>
 * }}}
 *
 * If the Raw-File header is an http[s] URI, then the file is downloaded and saved locally in the temporary directory.
 * If if it is a file: URI, then the local file is used.
 *
 *
 * A query request contains the logical plan in the body, as a plain text string. Example:
 *
 * {{{
    POST /query HTTP/1.1

    Reduce(SetMonoid(),
    Arg(RecordType(Seq(AttrType(name,StringType()),
    AttrType(title,StringType()),
    AttrType(year,IntType())),
    authors_99)),
    BoolConst(true),
    Select(BoolConst(true),
    Scan(authors,
    SetType(RecordType(Seq(AttrType(name,StringType()),
    AttrType(title,StringType()),
    AttrType(year,IntType())),
    authors_99)))))
 * }}}
 * @param executorArg scala or spark executor. Currently, on Scala executor is implemented.
 */
class RawRestServer(executorArg: String, storageDirCmdOption: Option[String]) extends SimpleRoutingApp with StrictLogging {

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

  def myExceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: ClientErrorException =>
        requestUri { uri =>
          logger.warn(s"Request to $uri could not be handled normally: $e")
          complete(StatusCodes.BadRequest, e.getMessage)
        }
      case e: InternalErrorException =>
        requestUri { uri =>
          logger.warn(s"Request to $uri could not be handled normally: $e")
          complete(StatusCodes.InternalServerError, e.getMessage)
        }
        // Any other exception will be handled by Spray and return a 500 status code.
    }

  def start(): Future[Bound] = {
    import RegisterRequestJsonSupport._
    val queryPath = "query"
    val registerPath = "register"
    val schemasPath = "schemas"
    logger.info(s"Listening on localhost:$port/{$registerPath,$queryPath}")
    val future: Future[Bound] = startServer("0.0.0.0", port = port) {
      handleExceptions(myExceptionHandler) {
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
                  complete( """ {"success" = True } """)
                }
              }
            } ~
            (path(schemasPath) & get) {
              headerValueByName("Raw-User") { user =>
                logger.info(s"Returning schemas for $user")
                val schemas: Seq[String] = rawServer.getSchemas(user)
                respondWithMediaType(MediaTypes.`application/json`) {
                  complete(schemas)
                }
              }
            }
        }
      }
    }
    future
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
