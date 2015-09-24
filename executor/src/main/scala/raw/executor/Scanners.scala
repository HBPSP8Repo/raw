package raw.executor

import java.io.InputStream
import java.nio.file.Files

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import raw.utils.Instrumented

import scala.collection.immutable.HashMap
import scala.collection.{AbstractIterator, Iterator, JavaConversions}
import scala.reflect._
import scala.reflect.runtime.universe._


object RawScanner {
  def apply[T:TypeTag:ClassTag](schema: RawSchema, m: Manifest[T]): RawScanner[T] = {
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

abstract class RawScanner[T: TypeTag: ClassTag](val schema: RawSchema) extends Iterable[T] {
  val tt = typeTag[T]
  val ct = classTag[T]
  // The default Iterable toString implementation prints the full contents
  override def toString() = s"${this.getClass}(${schema.toString}, tag:${typeTag[T]})"

  override def iterator: AbstractClosableIterator[T]
}

class SparkRawScanner[T: TypeTag: ClassTag](scanner: RawScanner[T], sc: SparkContext) extends RawScanner[T](scanner.schema) with StrictLogging {
  override def iterator: AbstractClosableIterator[T] = scanner.iterator

  def toRDD() = {
    val iter = scanner.iterator
    try {
      sc.parallelize(iter.toBuffer)
    } finally {
      logger.info("Closing iterator: " + iter.close())
      iter.close()
    }
  }
}


trait AbstractClosableIterator[A] extends AbstractIterator[A] with Iterator[A] {
  def close()
}

case class ClosableIterator[A](underlying: MappingIterator[A]) extends AbstractClosableIterator[A] {
  def hasNext = underlying.hasNext()

  def next() = underlying.next()

  def close() = underlying.close()
}

case class ClosableIteratorInputStream[A](underlying: Iterator[A], is: InputStream) extends AbstractClosableIterator[A] {
  def hasNext = underlying.hasNext()

  def next() = underlying.next()

  def close() = is.close()
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

class CsvRawScanner[T:ClassTag:TypeTag](schema: RawSchema, m: Manifest[T]) extends RawScanner[T](schema) with Instrumented with StrictLogging {
  import CsvRawScanner._

  val tag = typeTag[T]

  private[this] val csvSchema = CsvSchema.emptySchema().withSkipFirstDataRow(schema.properties.hasHeader().getOrElse(false))
  private[this] val ctor = {
    logger.info("Type manifest: " + m)
    val ctors = m.runtimeClass.getConstructors
    assert(ctors.size == 1, "Expected a single constructor. Found: " + ctors)
    ctors.head
  }
  private[this] var parTypes = ctor.getParameterTypes

  override def iterator: AbstractClosableIterator[T] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for CSV resource: $p. Properties: ${properties}")

    // TODO: Potential resource leak. Who closes the InputStream once the client code is done with the
    // iterator? There is no close() method on the Iterator interface
    val is: InputStream = Files.newInputStream(schema.dataFile)
    val iter = csvMapper
      .readerFor(classOf[Array[String]])
      .`with`(csvSchema)
      .readValues(is)
      .asInstanceOf[MappingIterator[Array[String]]]
    val scalaIter = JavaConversions.asScalaIterator(iter)
    val mappedIter: scala.Iterator[T] = scalaIter.map(v => newValue(v))
    new ClosableIteratorInputStream(mappedIter, is)
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

class JsonRawScanner[T:ClassTag:TypeTag](schema: RawSchema, m: Manifest[T]) extends RawScanner[T](schema) with StrictLogging with Iterable[T] {
  import JsonRawScanner._

  val tag = typeTag[T]

  /* http://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/ */
  override def iterator: AbstractClosableIterator[T] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for Json resource: $p. Properties: ${properties}, Manifest: $m, TyppeTag: ${typeTag[T]}")

    val is: InputStream = Files.newInputStream(schema.dataFile)
    val jp = jsonFactory.createParser(is)

    assert(jp.nextToken() == JsonToken.START_ARRAY)
    assert(jp.nextToken() == JsonToken.START_OBJECT)
    val iter: MappingIterator[T] = mapper.readValues(jp)(m)
    new ClosableIterator[T](iter)
  }
}
