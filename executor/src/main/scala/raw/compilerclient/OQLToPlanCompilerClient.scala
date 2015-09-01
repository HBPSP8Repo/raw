package raw.compilerclient

import java.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.slf4j.LoggerFactory
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.LogicalAlgebraParser

import scala.io.Source

/**
 * Compiles an OQL string into a LogicalAlgebraNode. Performs two tasks:
 * - Query the LDB C backend to convert the OQL to a logical plan string.
 * - Parses the logical plan into a LogicalAlgebraNode
 */
object OQLToPlanCompilerClient extends StrictLogging {
  val loggerQueries = LoggerFactory.getLogger("raw.queries")
  val compileServerUrlProperty = "raw.compile.server.host"
  val serverUrl: String = System.getProperty(compileServerUrlProperty, "http://localhost:5001/raw-plan")

  def apply(oql: String): Either[String, LogicalAlgebraNode] = {
    val logicalPlan = getRestContent(serverUrl, oql)
    //    logger.info(s"Raw logical algebra:\n$logicalPlan")
    loggerQueries.info(s"LDB generated logical plan:\n${logicalPlan}")
    LogicalAlgebraParser(logicalPlan)
  }

  def getRestContent(url: String, oql: String): String = {
    val httpClient = HttpClients.createDefault()
    val post = new HttpPost(url)
    val nameValuePairs = new util.ArrayList[BasicNameValuePair](1)
    nameValuePairs.add(new BasicNameValuePair("oql", oql));
    post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
    logger.info(s"Query $oql, Compilation service: ${post.getURI}")
    val httpResponse = httpClient.execute(post)
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).mkString
      inputStream.close
    }
    httpClient.close()
    return content
  }
}
