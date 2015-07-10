package raw.executionserver

import java.net.URL

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopOption, ScallopConf, Scallop}
import raw.datasets.patients.PatientsDataset
import raw.datasets.publications.PublicationsDataset
import spray.http.{MediaTypes, StatusCodes}
import spray.routing.SimpleRoutingApp

object Main extends SimpleRoutingApp with StrictLogging with ResultConverter {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Scala/Spark OQL execution server")
      val dataset:ScallopOption[String] = opt[String]("dataset", default = Some("publications"), short = 'd')
    }

    // Do not do any unncessary initialization until command line arguments (dataset) is validated.
    lazy val rawClassLoader = {
      val cl = new RawMutableURLClassLoader(new Array[URL](0), Main.getClass.getClassLoader)
      logger.info("Created raw class loader: " + cl)
      cl
    }

    lazy val sc = {
      Thread.currentThread().setContextClassLoader(rawClassLoader)
      logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
      new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
    }

    val ds = Conf.dataset.get match {
      case Some("publications") => new PublicationsDataset(sc)
      case Some("patients") => new PatientsDataset(sc)
      case _ => println(s"Unknown dataset: ${Conf.dataset}"); return
    }

    val executionServer = new ExecutionServer(rawClassLoader, sc)
    val port = 54321



    implicit val system = ActorSystem("simple-routing-app")
    val executePath = "execute"
    logger.info(s"Listening on localhost:$port/$executePath")
    startServer("0.0.0.0", port = port) {
      (path(executePath) & post) {
        entity(as[String]) { query =>
          returnValue(query) match {
            case Left(error) =>
              logger.warn(s"Failed to process request: $error")
              complete(StatusCodes.BadRequest, error)
            case Right(result) =>
              logger.info("Query succeeded. Returning result: " + result.take(10))
              respondWithMediaType(MediaTypes.`application/json`) {
                complete(result)
              }
          }
        }
      }
    }

    def returnValue(query: String): Either[String, String] = {
      // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
      // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
      // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
      // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
      val cleanedQuery = query.trim.replaceAll("\\s+", " ")
      executionServer.execute(cleanedQuery, ds.accessPaths)
        .right.map(convertToJson(_))
    }
  }
}
