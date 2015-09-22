package raw

import java.io.InputStream
import java.nio.file.Files

import com.fasterxml.jackson.core.{JsonToken, JsonFactory}
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.datasets.patients.Patient

import scala.collection.JavaConversions
import scala.reflect.Manifest


class JsonLoader[T](resource: String)(implicit m: Manifest[T]) extends StrictLogging with Iterable[T] {
  private[this] final val jsonFactory = new JsonFactory()
  private[this] final val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  /* http://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/ */
  override def iterator: Iterator[T] = {
    logger.info(s"Loading JSON resource: $resource")
    val is: InputStream = Resources.getResource(resource).openStream()
    val jp = jsonFactory.createParser(is)

    assert(jp.nextToken() == JsonToken.START_ARRAY)
    assert(jp.nextToken() == JsonToken.START_OBJECT)
    val iter: MappingIterator[T] = mapper.readValues(jp)(m)
    JavaConversions.asScalaIterator(iter)
  }
}

class JacksonStreamingParserTest extends FunSuite with StrictLogging with BeforeAndAfterAll {

  test("stream JSON") {
    val res = new JsonLoader[Patient]("data/patients/patients.json")

    res.foreach(p => logger.info("Read patient: " + p))
  }
}


