package raw.datasets

import java.io.InputStream
import java.nio.file.Files

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import raw.executor.RawSchema

import scala.reflect._

object JsonLoader extends StrictLogging {
  private[this] final val jsonFactory = new JsonFactory()
  private[this] final val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def load[T](resource: String)(implicit m: Manifest[T]): T = {
    logger.info(s"Loading JSON resource: $resource")
    val is: InputStream = Resources.getResource(resource).openStream()
    try {
      val jp = jsonFactory.createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }

  def loadAbsolute[T](schema: RawSchema)(implicit m: Manifest[T]): T = {
    val p = schema.dataFile
    logger.info(s"Loading JSON resource: $p")
    val is: InputStream = Files.newInputStream(p)
    try {
      val jp = jsonFactory.createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }
}

