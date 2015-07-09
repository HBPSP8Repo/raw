package raw.executionserver

import java.net.URL

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.datasets.patients.PatientsDataset
import spray.http.{MediaTypes, StatusCodes}
import spray.routing.SimpleRoutingApp


object Main extends SimpleRoutingApp with StrictLogging with ResultConverter {
  val rawClassLoader = {
    val cl = new RawMutableURLClassLoader(new Array[URL](0), Main.getClass.getClassLoader)
    logger.info("Created raw class loader: " + cl)
    cl
  }

  val sc = {
    Thread.currentThread().setContextClassLoader(rawClassLoader)
    logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
    new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
  }

//  val pubsDS = new PublicationsDataset(sc)
  val ds = new PatientsDataset(sc)

  val executionServer = new ExecutionServer(rawClassLoader, sc)
  val port = 54321


  def main(args: Array[String]) {
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