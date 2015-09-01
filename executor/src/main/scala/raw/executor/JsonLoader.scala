package raw.executor

import java.io.InputStream
import java.nio.file.{Files, Path}

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging

import scala.reflect._

object JsonLoader extends StrictLogging {
  private[this] final val jsonFactory = new JsonFactory()
  private[this] final val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def load[T](resource: String)(implicit m: Manifest[T]): T = {
    logger.info(s"Loading resource: $resource")
    val is: InputStream = Resources.getResource(resource).openStream()
    try {
      val jp = jsonFactory.createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }

  def loadAbsolute[T](p: Path)(implicit m: Manifest[T]): T = {
    logger.info(s"Loading resource: $p")
    val is: InputStream = Files.newInputStream(p)
    try {
      val jp = jsonFactory.createParser(is)
      mapper.readValue(jp)(m)
    } finally {
      is.close()
    }
  }
}

