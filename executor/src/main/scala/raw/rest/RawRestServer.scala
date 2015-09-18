package raw.rest

import java.nio.file.{Paths, Files}

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopConf, ScallopOption}
import raw.datasets.AccessPath
import raw.executor.{RawSchema, CodeGenerationExecutor, StorageManager}
import raw.spark._
import spray.can.Http.Bound
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing.SimpleRoutingApp

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
class RawRestServer(executorArg: String) extends SimpleRoutingApp with StrictLogging {

  val sc: Option[SparkContext] = executorArg match {
    case "scala" => logger.info("Using Scala-only executor"); None
    case "spark" =>
      logger.info("Using Spark")
      lazy val sc: SparkContext = {
        Thread.currentThread().setContextClassLoader(CodeGenerationExecutor.rawClassloader)
        logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
        new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
      }
      Some(sc)
    case exec@_ =>
      throw new IllegalArgumentException(s"Invalid executor: $exec. Valid options: [scala, spark]")
  }

  final val port = 54321
  implicit val system = ActorSystem("simple-routing-app")

  def start(): Future[Bound] = {
    import RegisterRequestJsonSupport._
    val queryPath = "query"
    val registerPath = "register"
    logger.info(s"Listening on localhost:$port/{$registerPath,$queryPath}")
    startServer("0.0.0.0", port = port) {
      (path(queryPath) & post) {
        headerValueByName("Raw-User") { rawUser =>
          entity(as[String]) { query =>
            try {
              val result = doQuery(query, rawUser)
              logger.info("Query succeeded. Returning result: " + result.take(30))
              respondWithMediaType(MediaTypes.`application/json`) {
                complete(result)
              }
            } catch {
              case ex: RuntimeException =>
                logger.warn(s"Failed to process request", ex)
                complete(StatusCodes.BadRequest, ex.getMessage)
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
            try {
              logger.info(s"$user, $schemaName, $dataDir")
              //                    doRegisterRequest(schemaName, xmlSchema, fileURI, rawUser)
              StorageManager.registerSchema(schemaName, dataDir, user)
              val schemas = StorageManager.listSchemas(user)
              logger.info("Schemas: " + schemas)
              respondWithMediaType(MediaTypes.`application/json`) {
                complete( """ {"success" = True } """)
              }
            } catch {
              case ex: RuntimeException =>
                logger.warn(s"Failed to process request.", ex)
                complete(StatusCodes.BadRequest, ex.getMessage)
            }
          }
        }
    }
  }

  def stop(): Unit = {
    logger.info("Shutting down")
    system.shutdown()
  }


  def doQuery(query: String, rawUser: String): String = {
    // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
    // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
    // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
    // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
    val cleanedQuery = query.trim.replaceAll("\\s+", " ")

    val schemas = StorageManager.listSchemas(rawUser)
    logger.info("Found schemas: " + schemas.mkString(", "))
    val accessPaths: Seq[AccessPath[_]] = schemas.map(name => {
      val schema: RawSchema = StorageManager.getSchema(rawUser, name)
      logger.info(s"Loading access path for schema: $schema")
      CodeGenerationExecutor.loadAccessPath(name, schema)
    })
    CodeGenerationExecutor.query(cleanedQuery, accessPaths)
  }

  //  def doRegisterRequest(schemaName: String, xmlSchema: String, fileURI: String, rawUser: String) = {
  //    logger.info(s"Registering schema: $schemaName, file: $fileURI, user: $rawUser")
  //    val uri = new URI(fileURI)
  //    val localFile = if (uri.getScheme().startsWith("http")) {
  //      logger.info("toURL: " + uri.toURL)
  //      logger.info("toURL.getFile: " + uri.toURL.getFile)
  //      val localPath = Files.createTempFile(schemaName, schemaName + ".json")
  //      val is = uri.toURL.openStream()
  //      logger.info(s"Downloading file $fileURI to $localPath")
  //      Files.copy(is, localPath, StandardCopyOption.REPLACE_EXISTING)
  //      is.close()
  //      localPath
  //    } else {
  //      Paths.get(uri)
  //    }
  //    logger.info(s"Registering file: $localFile with schema: $schemaName")
  //    CodeGenerationExecutor.registerAccessPath(schemaName, new StringReader(xmlSchema), localFile)
  //  }
}

object RawRestServerMain extends StrictLogging {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Scala/Spark OQL execution server")
      val executor: ScallopOption[String] = opt[String]("executor", default = Some("scala"), short = 'e')
    }
    val restServer = new RawRestServer(Conf.executor())

    //    logger.info("Directory: " + ConfigFactory.load().getString("raw.datadir"))
    restServer.start()
  }
}
