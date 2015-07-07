package raw.executionserver

import java.io.InputStream

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import raw.publications.{Author, Publication}

import scala.collection.JavaConversions
import scala.reflect.ClassTag


object ScalaDataSet extends StrictLogging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def loadJSON[T](resource: String)(implicit ct: ClassTag[T]): List[T] = {
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

  val authors: List[Author] = loadJSON[Author]("data/publications/authors.json")
  val publications: List[Publication] = loadJSON[Publication]("data/publications/publications.json")
}

