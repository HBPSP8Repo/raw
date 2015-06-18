package raw.compilerclient

import java.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{HttpClients, DefaultHttpClient}
import org.apache.http.message.BasicNameValuePair
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.{LogicalAlgebraPrettyPrinter, LogicalAlgebraParser}

import scala.io.Source

class QueryCompilerClient extends StrictLogging {
  val compileServerUrlProperty = "raw.compile.server.host"
  val serverUrl: String = System.getProperty(compileServerUrlProperty, "http://192.168.1.32:5000/raw-plan")

  def compile(oql: String): Either[String, LogicalAlgebraNode] = {
    val logicalPlan = doQuery(oql)
    logger.info("Parsing logical algebra")
    LogicalAlgebraParser(logicalPlan)
  }

  private def doQuery(oql: String): String = {
    val content = getRestContent(serverUrl, oql)
    logger.info("Plan: {}", content)
    content
  }

  /**
   * Returns the text content from a REST URL. Returns a blank String if there
   * is a problem.
   */
  def getRestContent(url: String, oql:String): String = {
    val httpClient = HttpClients.createDefault()
    val post = new HttpPost(url)
    val nameValuePairs = new util.ArrayList[BasicNameValuePair](1)
    nameValuePairs.add(new BasicNameValuePair("oql", oql));
    post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
    logger.info(s"Request: $post, Query: $oql")
    val httpResponse = httpClient.execute(post)
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.close()
    return content
  }
}
