package raw.executionserver

import java.io.InputStream

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions
import scala.reflect.ClassTag

object JsonLoader extends StrictLogging {
  private[this] val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def load[T](resource: String)(implicit ct: ClassTag[T]): List[T] = {
    logger.info(s"Loading resource: $resource")
    val is: InputStream = Resources.getResource(resource).openStream()
    try {
      val jp = new JsonFactory().createParser(is)
      val reader: MappingIterator[T] = mapper.readValues(jp, ct.runtimeClass.asInstanceOf[Class[T]])
      return JavaConversions.asScalaIterator(reader).toList
    } finally {
      is.close()
    }
  }
}

