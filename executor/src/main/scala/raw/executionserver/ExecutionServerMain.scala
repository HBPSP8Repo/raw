package raw.executionserver

import java.net.URL

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.publications.{Author, Publication}
import spray.http.{MediaTypes, RequestProcessingException, StatusCodes}
import spray.routing.SimpleRoutingApp


object Main extends SimpleRoutingApp with StrictLogging with ResultConverter {
  implicit val system = ActorSystem("simple-routing-app")

  val rawClassLoader = {
    val cl = new RawMutableURLClassLoader(new Array[URL](0), Main.getClass.getClassLoader)
    logger.info("Created raw class loader: " + cl)
    cl
  }

  val sc = {
    Thread.currentThread().setContextClassLoader(rawClassLoader)
    logger.info("Starting SparkContext with configuration:\n{}", SharedSparkContext.conf.toDebugString)
    new SparkContext("local[4]", "test", SharedSparkContext.conf)
  }

  val authorsRDD = SharedSparkContext.newRDDFromJSON[Author](ScalaDataSet.authors, sc)
  val publicationsRDD = SharedSparkContext.newRDDFromJSON[Publication](ScalaDataSet.publications, sc)

  val executionServer = new ExecutionServer(rawClassLoader)
  val port = 54321

  def main(args: Array[String]) {
    implicit val system = ActorSystem("simple-routing-app")
    val executePath = "execute"
    logger.info(s"Listening on localhost:$port/$executePath")
    startServer("localhost", port = port) {
      (path(executePath) & post) {
        entity(as[String]) { query =>
          returnValue(query) match {
            case Some(t) => respondWithMediaType(MediaTypes.`application/json`) {
              complete {
                t
              }
            }
            case None => failWith(new RequestProcessingException(StatusCodes.InternalServerError))
          }
        }
      }
    }
  }

  def returnValue(query: String): Option[String] = {
    // If the query string is too big (threshold somewhere between 13K and 96K), the compilation will fail with
    // an IllegalArgumentException: null. The query plans received from the parsing server include large quantities
    // of whitespace which are used for indentation. We remove them as a workaround to the limit of the string size.
    // But this can still fail for large enough plans, so check if spliting the lines prevents this error.
    val cleanedQuery = query.trim.replaceAll("\\s+", " ")
    val answer = executionServer.execute(cleanedQuery, authorsRDD, publicationsRDD, sc)
    Some(convertToJson(answer))
  }
}