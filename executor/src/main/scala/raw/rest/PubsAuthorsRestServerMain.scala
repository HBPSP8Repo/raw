//package raw.rest
//
//import akka.actor.ActorSystem
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.spark.SparkContext
//import org.rogach.scallop.{ScallopConf, ScallopOption}
//import raw.datasets.AccessPath
//import raw.executor.{QueryCompilerClient, RawMutableURLClassLoader, ResultConverter}
//import raw.spark.DefaultSparkConfiguration
//import spray.http.{MediaTypes, StatusCodes}
//import spray.routing.SimpleRoutingApp
//
///**
// * Exposes a /execute service which takes a logical plan and executes it locally.
// * The access paths must be pre-loaded and match the ones referenced in the logical plan.
// * This was partially made obsolete by RawRestServerMain, which exposes /register and /query
// * services, so does not assume a static dataset
// */
//object PubsAuthorsRestServerMain extends SimpleRoutingApp with StrictLogging with ResultConverter {
//  def main(args: Array[String]) {
//    object Conf extends ScallopConf(args) {
//      banner("Scala/Spark OQL execution server")
//      val dataset: ScallopOption[String] = opt[String]("dataset", default = Some("publications"), short = 'd')
//      val executor: ScallopOption[String] = opt[String]("executor", default = Some("scala"), short = 'e')
//    }
//
//    // Do not do any unnecessary initialization until command line arguments (dataset) is validated.
//    lazy val rawClassLoader = {
//      val cl = new RawMutableURLClassLoader(PubsAuthorsRestServerMain.getClass.getClassLoader)
//      logger.info("Created raw class loader: " + cl)
//      cl
//    }
//
//    val accessPaths = Conf.executor() match {
//      case "scala" =>
//        AccessPath.loadScalaDataset(Conf.dataset())
//      case "spark" =>
//        lazy val sc: SparkContext = {
//          Thread.currentThread().setContextClassLoader(rawClassLoader)
//          logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
//          new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
//        }
//        AccessPath.loadSparkDataset(Conf.dataset(), sc)
//
//      case exec@_ =>
//        println(s"Invalid executor: $exec. Valid options: [scala, spark]")
//        return
//    }
//
//
//    val executionServer = new QueryCompilerClient(rawClassLoader)
//    val port = 54321
//    implicit val system = ActorSystem("simple-routing-app")
//    val executePath = "execute"
//    logger.info(s"Listening on localhost:$port/$executePath")
//    startServer("0.0.0.0", port = port) {
//      (path(executePath) & post) {
//        entity(as[String]) { query =>
//          try {
//            val result = returnValue(query)
//            logger.info("Query succeeded. Returning result: " + result.take(30))
//            respondWithMediaType(MediaTypes.`application/json`) {
//              complete(result)
//            }
//          } catch {
//            case ex: RuntimeException =>
//              logger.warn(s"Failed to process request: $ex")
//              complete(StatusCodes.BadRequest, ex.getMessage)
//          }
//        }
//      }
//    }
//
//    def returnValue(query: String): String = {
//      // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
//      // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
//      // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
//      // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
//      val cleanedQuery = query.trim.replaceAll("\\s+", " ")
//      val compiledQuery = executionServer.compileLogicalPlan(cleanedQuery, accessPaths)
//      val res = compiledQuery.computeResult
//      convertToJson(res)
//    }
//  }
//}
