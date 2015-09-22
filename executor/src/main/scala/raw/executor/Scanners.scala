package raw.executor

import com.fasterxml.jackson.core.{JsonToken, JsonFactory}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect._
import scala.reflect.runtime.universe._

import java.io.InputStream
import java.nio.file.Files

import com.fasterxml.jackson.databind.{ObjectMapper, MappingIterator}
import com.fasterxml.jackson.dataformat.csv.{CsvSchema, CsvParser, CsvMapper}
import com.typesafe.scalalogging.StrictLogging
import raw.utils.Instrumented

import scala.collection.JavaConversions
import scala.collection.immutable.HashMap


object RawScanner {
  def apply[T](schema: RawSchema, m: Manifest[T])(implicit tag: TypeTag[T]): RawScanner[T] = {
    val p = schema.dataFile
    if (p.toString.endsWith(".json")) {
      new JsonRawScanner(schema, m)
    } else if (p.toString.endsWith(".csv")) {
      new CsvRawScanner(schema, m)
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + p)
    }
  }
}

abstract class RawScanner[T](val schema: RawSchema)(implicit val tag: TypeTag[T]) extends Iterable[T] {
  // The default Iterable toString implementation prints the full contents
  override def toString() = s"${this.getClass}(${schema.toString})"
}

object CsvRawScanner {
  private val mapperFunctions = HashMap[String, (String) => AnyRef](
    "java.lang.String" -> ((v: String) => v),
    "int" -> ((v: String) => Integer.valueOf(v)),
    "double" -> ((v: String) => java.lang.Double.valueOf(v)),
    "float" -> ((v: String) => java.lang.Float.valueOf(v))
  )

  // TODO: thread-safe?
  private final val csvMapper = {
    val m = new CsvMapper()
    m.enable(CsvParser.Feature.WRAP_AS_ARRAY)
    m
  }
}

class CsvRawScanner[T](schema: RawSchema, m: Manifest[T])(implicit tag: TypeTag[T]) extends RawScanner[T](schema) with Instrumented with StrictLogging {

  import CsvRawScanner._

  private[this] val csvSchema = CsvSchema.emptySchema().withSkipFirstDataRow(schema.properties.hasHeader().getOrElse(false))
  private[this] val ctor = {
    logger.info("Type manifest: " + m)
    val ctors = m.runtimeClass.getConstructors
    assert(ctors.size == 1, "Expected a single constructor. Found: " + ctors)
    ctors.head
  }
  private[this] var parTypes = ctor.getParameterTypes

  override def iterator: Iterator[T] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for CSV resource: $p. Properties: ${properties.properties}")
    val iter: Iterator[Array[String]] = newCsvIterator()
    iter.map(v => newValue(v))
  }

  private[this] def newCsvIterator(): Iterator[Array[String]] = {
    // TODO: Potential resource leak. Who closes the InputStream once the client code is done with the
    // iterator? There is no close() method on the Iterator interface
    val is: InputStream = Files.newInputStream(schema.dataFile)
    val iter = csvMapper
      .readerFor(classOf[Array[String]])
      .`with`(csvSchema)
      .readValues(is)
      .asInstanceOf[MappingIterator[Array[String]]]
    JavaConversions.asScalaIterator(iter)
  }

  // Buffer reused in calls to newValue().
  private[this] val args = new Array[AnyRef](ctor.getParameterCount)

  // TODO: Analyze the schema at construction of this instance and create an array with the functions needed to parse each
  // element, to avoid the lookup in every call to newValue
  private[this] def newValue(data: Array[String]): T = {
    var i = 0
    // Note: This can be optimized by code-generating the construction code and avoiding the per-instance lookup
    // of the conversion functions and avoiding the use of reflection.
    while (i < ctor.getParameterCount) {
      mapperFunctions.get(parTypes(i).getName) match {
        case Some(f) => args.update(i, f(data(i)))
        case None => {
          logger.warn("Unknown type: " + parTypes(i))
          // Try using the string value found on the CSV file
          args.update(i, data(i))
        }
      }
      i += 1
    }
    ctor.newInstance(args: _*).asInstanceOf[T]
  }
}


object JsonRawScanner {
  // TODO: Is this thread-safe?
  private final val jsonFactory = new JsonFactory()
  private final val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
}

class JsonRawScanner[T](schema: RawSchema, m: Manifest[T])(implicit tag: TypeTag[T]) extends RawScanner[T](schema) with StrictLogging with Iterable[T] {
  import JsonRawScanner._

  /* http://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/ */
  override def iterator: Iterator[T] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for Json resource: $p. Properties: ${properties.properties}")

    val is: InputStream = Files.newInputStream(schema.dataFile)
    val jp = jsonFactory.createParser(is)

    assert(jp.nextToken() == JsonToken.START_ARRAY)
    assert(jp.nextToken() == JsonToken.START_OBJECT)
    val iter: MappingIterator[T] = mapper.readValues(jp)(m)
    JavaConversions.asScalaIterator(iter)
  }
}


