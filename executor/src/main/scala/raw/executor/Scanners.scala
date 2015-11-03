package raw.executor

import java.io._
import java.nio.charset.{Charset, StandardCharsets}

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import com.fasterxml.jackson.databind.{MappingIterator, ObjectMapper}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.base.Charsets
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.{LineIterator, IOUtils}
import org.apache.spark.SparkContext
import raw.utils.Instrumented

import scala.collection.immutable.HashMap
import scala.collection.{AbstractIterator, Iterator, JavaConversions}
import scala.io.{Codec, Source}
import scala.reflect._
import scala.reflect.runtime.universe._


object RawScanner extends StrictLogging {
  def apply[T: TypeTag : ClassTag : Manifest](schema: RawSchema): RawScanner[T] = {
    if (schema.fileType == "json") {
      new JsonRawScanner(schema)
    } else if (schema.fileType == "csv") {
      new CsvRawScanner(schema)
    } else if (schema.fileType == "text") {
      // T must be string
      logger.info(s"Type: ${typeTag[T]}")
      new TextRawScanner(schema).asInstanceOf[RawScanner[T]]
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + schema.schemaFile)
    }
  }
}

abstract class RawScanner[T: TypeTag : ClassTag](val schema: RawSchema) extends Iterable[T] {
  val tt = typeTag[T]
  val ct = classTag[T]

  // The default Iterable toString implementation prints the full contents
  override def toString() = s"${this.getClass}(${schema.toString}, tag:${typeTag[T]})"

  override def iterator: AbstractClosableIterator[T]
}

class SparkRawScanner[T: TypeTag : ClassTag](scanner: RawScanner[T], sc: SparkContext) extends RawScanner[T](scanner.schema) with StrictLogging {
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


case class ClosableIterator[A](underlying: Iterator[A], is: Closeable) extends AbstractClosableIterator[A] {
  def hasNext = underlying.hasNext

  def next() = underlying.next()

  def close() = is.close()

  override def toString = s"${underlying.getClass}"
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

class CsvRawScanner[T: ClassTag : TypeTag : Manifest](schema: RawSchema) extends RawScanner[T](schema) with Instrumented with StrictLogging {

  import CsvRawScanner._

  val tag = typeTag[T]

  private[this] val csvSchema =
    if (schema.properties == null)
      CsvSchema.emptySchema()
    else
      CsvSchema.emptySchema()
        .withSkipFirstDataRow(schema.properties.hasHeader().getOrElse(false))
        .withColumnSeparator(schema.properties.delimiter().getOrElse(','))
  //  private[this] val csvSchema = {
  //  Name:String, year:Int, office:String, department:String
  //    CsvSchema.builder()
  //      .addColumn("Name")
  //      .addColumn("year", CsvSchema.ColumnType.NUMBER)
  //      .addColumn("office")
  //      .addColumn("department")
  //      .setSkipFirstDataRow(schema.properties.hasHeader().getOrElse(false))
  //      .build()
  //  }
  private[this] val ctor = {
    val m = manifest[T]
    val ctors = m.runtimeClass.getConstructors
    assert(ctors.size == 1, "Expected a single constructor. Found: " + ctors)
    ctors.head
  }
  private[this] var parTypes = ctor.getParameterTypes

  // Precompute the functions used to convert each value read from the file into Scala objects. This should save
  // some time during the parsing of the file
  private[this] val mapFunctions: Array[(String) => AnyRef] = {
    val functions = new Array[(String) => AnyRef](ctor.getParameterCount)
    var i = 0
    while (i < ctor.getParameterCount) {
      mapperFunctions.get(parTypes(i).getName) match {
        case Some(f) => functions.update(i, f)
        case None => {
          logger.warn("Unknown type: " + parTypes(i))
          // Try using the string value found on the CSV file (identity function)
          functions.update(i, s => s)
        }
      }
      i += 1
    }
    logger.info(s"Resource: ${schema.name}, Conversion functions: $functions")
    functions
  }

  override def iterator: AbstractClosableIterator[T] = {
    val p = schema.dataFile
    logger.info(s"Creating iterator for CSV resource: $p")

    val is: Reader = new InputStreamReader(schema.dataFile.openInputStream(), StandardCharsets.UTF_8)
    val iter = csvMapper
      .readerFor(classOf[Array[String]])
      .`with`(csvSchema)
      .readValues(is)
      .asInstanceOf[MappingIterator[Array[String]]]
    val scalaIter = JavaConversions.asScalaIterator(iter)
    val mappedIter: scala.Iterator[T] = scalaIter.map(v => newValue(v))
    new ClosableIterator(mappedIter, is)
  }

  // Buffer reused by calls to newValue().
  private[this] val args = new Array[AnyRef](ctor.getParameterCount)

  // Can be sped by using a code-generator callback.
  private[this] def newValue(data: Array[String]): T = {
    var i = 0
    while (i < ctor.getParameterCount) {
      val value = data(i).trim
      val mapperFunction = mapFunctions(i)
      args.update(i, mapperFunction(value))
      i += 1
    }
    ctor.newInstance(args: _*).asInstanceOf[T]
  }
}

object JsonRawScanner {
  private final val jsonFactory = new JsonFactory()
  private final val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
}

class JsonRawScanner[T: ClassTag : TypeTag : Manifest](schema: RawSchema) extends RawScanner[T](schema) with StrictLogging with Iterable[T] {

  import JsonRawScanner._

  val tag = typeTag[T]

  /* http://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/ */
  override def iterator: AbstractClosableIterator[T] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for Json resource: $p. Properties: ${properties}, Manifest: ${manifest[T]}, TyppeTag: ${typeTag[T]}")

    // TODO: The jsonFactory can generate a parser that manages the input stream if we pass it the file reference instead
    // of the inputstream. Check if this works properly and if so, remove the ClosableIterator mechanism.
    val is: InputStream = schema.dataFile.openInputStream()
    val jp = jsonFactory.createParser(is)

    assert(jp.nextToken() == JsonToken.START_ARRAY)
    val nt = jp.nextToken()
    assert(nt == JsonToken.START_OBJECT || nt == JsonToken.START_ARRAY, "Found: " + nt)
    val iter: Iterator[T] = JavaConversions.asScalaIterator(mapper.readValues[T](jp))
    new ClosableIterator[T](iter, is)
  }
}


object TextRawScanner extends StrictLogging {

  import org.mozilla.universalchardet.UniversalDetector

  // How much to read of the file in the attempt to detect the character encoding.
  final val MAX_READAHEAD = 32 * 1024

  // Tries to detect the character encoding by reading a section of the file. If no encoding is detect, assumes UTF-8
  def detect(is: BufferedInputStream): String = {
    val buf = new Array[Byte](4096)
    // If we read more than this value, the mark becomes invalid and we cannot reset the stream. So the loop below
    // reads only up to MAX_READAHEAD
    is.mark(MAX_READAHEAD)

    val detector = new UniversalDetector(null)
    var totalRead = 0 // How much we have read so far of the input stream
    var bytesRead = 0 // How much was read in the current iteration.
    do {
      val maxNextRead = Math.min(buf.size, MAX_READAHEAD - totalRead)
      bytesRead = is.read(buf, 0, maxNextRead)
      if (bytesRead > 0) {
        detector.handleData(buf, 0, bytesRead)
        totalRead += bytesRead
      }
    } while (bytesRead > 0 && !detector.isDone())
    detector.dataEnd()
    is.reset()

    val encoding = detector.getDetectedCharset()
    detector.reset()
    if (encoding == null) {
      logger.warn("No encoding detected. Trying UTF-8")
      "UTF-8"
    } else {
      // The detector may return UTF-16LE or UTF-16BE, but in some input files using an encoding with explicit byte order
      // will result in a failure. Instead, we use UTF-16 which is able to detect the BOM if present and uses big endian
      // otherwise.
      // http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html
      if (encoding.startsWith("UTF-16")) {
        "UTF-16"
      } else {
        encoding
      }
    }
  }
}

class TextRawScanner(schema: RawSchema) extends RawScanner[String](schema) with StrictLogging with Iterable[String] {
  override def iterator: AbstractClosableIterator[String] = {
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Creating iterator for text resource: $p. Properties: ${properties}")
    val is = new BufferedInputStream(schema.dataFile.openInputStream(), 1024)
    val charset = TextRawScanner.detect(is)
    val cs = Charset.forName(charset)
    logger.info(s"Loading file using charset: $cs")
    val source = Source.fromInputStream(is)(Codec(cs)).getLines()
    new ClosableIterator(source, is)
  }
}


