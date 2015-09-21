package raw.executor

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{ObjectMapper, MappingIterator}
import com.fasterxml.jackson.dataformat.csv.{CsvSchema, CsvMapper, CsvParser}
import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.Timer
import raw.utils.Instrumented

import scala.collection.JavaConversions
import scala.collection.immutable.HashMap
import scala.reflect._

case class Student(name: String, birthYear: Int, office: String, department: String)

object CsvLoader extends StrictLogging with Instrumented {

  private[this] val csvLoadTimer: Timer = metrics.timer("load:csv")

  private[this] final val csvMapper = new CsvMapper()
  csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)


  val mapperFunctions = HashMap[String, (String) => AnyRef](
    "java.lang.String" -> ((v: String) => v),
    "int" -> ((v: String) => Integer.valueOf(v)),
    "double" -> ((v: String) => java.lang.Double.valueOf(v)),
    "float" -> ((v: String) => java.lang.Float.valueOf(v))
  )

  private[this] def newCsvIterator(is: InputStream, properties: SchemaProperties): Iterator[Array[String]] = {
    val csvSchema = CsvSchema.emptySchema().withSkipFirstDataRow(properties.hasHeader().getOrElse(false))
    val iter = csvMapper
      .readerFor(classOf[Array[String]])
      .`with`(csvSchema)
      .readValues(is)
      .asInstanceOf[MappingIterator[Array[String]]]
    JavaConversions.asScalaIterator(iter)
  }

  def loadAbsolute[T](schema: RawSchema)(implicit m: Manifest[T]): T = {
    val start = Stopwatch.createStarted()
    val p = schema.dataFile
    val properties = schema.properties
    logger.info(s"Loading CSV resource: $p. Properties: ${properties.properties}")

    val typeArgs: List[Predef.Manifest[_]] = m.typeArguments
    assert(typeArgs.size == 1, "Expected a single type argument. Found: " + typeArgs)
    val innerType: Predef.Manifest[_] = typeArgs.head

    val ctors = innerType.runtimeClass.getConstructors
    assert(ctors.size == 1, "Expected a single constructor. Found: " + ctors)
    val ctor = ctors.head

    var parTypes = ctor.getParameterTypes
    val is: InputStream = Files.newInputStream(p)
    try {
      val iter = newCsvIterator(is, schema.properties)
      val args = new Array[AnyRef](ctor.getParameterCount)
      val res = iter.map(v => {
        var i = 0
        // Note: This can be optimized by code-generating the construction code and avoiding the per-instance lookup
        // of the conversion functions and avoiding the use of reflection.
        while (i < ctor.getParameterCount) {
          mapperFunctions.get(parTypes(i).getName) match {
            case Some(f) => args.update(i, f(v(i)))
            case None => {
              logger.warn("Unknown type: " + parTypes(i))
              // Try using the string value found on the CSV file
              args.update(i, v(i))
            }
          }
          i += 1
        }
        ctor.newInstance(args: _*)
      }
      ).toList
      val result = res.asInstanceOf[T]
      val loadTime = start.elapsed(TimeUnit.MILLISECONDS)
      logger.info("Loaded " + schema.dataFile + " in " + loadTime + "ms")
      csvLoadTimer.update(loadTime, TimeUnit.MILLISECONDS)
      result
    } finally {
      is.close()
    }
  }
}
