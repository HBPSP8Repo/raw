package raw.executor

import java.io.StringWriter
import java.lang.reflect.Field

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.ImmutableMultiset
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.LoggerFactory

import scala.collection.{Bag, JavaConversions}

/* Used to limit the size of the responses sent to the REST client.
 */
class BoundedStringWriter(val maxSize: Int) extends StringWriter(4096) {

  private[this] def checkLength(appendSize: Int) {
    if (super.getBuffer.length() + appendSize > maxSize) {
      throw new IllegalStateException("Output exceeds maximum size: " + maxSize)
    }
  }

  override def write(c: Int) {
    checkLength(1)
    super.write(c.toChar)
  }

  override def write(cbuf: Array[Char], off: Int, len: Int) {
    checkLength(len)
    super.write(cbuf, off, len)
  }

  override def write(str: String) {
    checkLength(str.length)
    super.write(str)
  }

  override def write(str: String, off: Int, len: Int) {
    checkLength(len)
    super.write(str, off, len)
  }
}


trait ResultConverter extends StrictLogging {
  val loggerQueries = LoggerFactory.getLogger("raw.queries")

  val mapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    //    om.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    om.setSerializationInclusion(Include.NON_EMPTY)
    om
  }

  def prettyPrintTree(tree: String): String = {
    val json = mapper.readValue(tree, classOf[Any])
    val sw = new StringWriter()
    mapper.writerWithDefaultPrettyPrinter().writeValue(sw, json)
    sw.toString
  }

  def convertToJson(res: Any): String = {
    //    logger.info("Converting result: " + res.getClass)
    //    val value = convertToCollection(res)
    //    logger.info("Converted to: " + value.getClass)
    val size = ConfigFactory.load().getInt("raw.max-rest-response-size")
    logger.info("Creating buffer with max size: " + size)
    val sw = new BoundedStringWriter(size)
    mapper.writeValue(sw, res)
    sw.toString
  }

  def convertToJsonNode(res: Any): JsonNode = {
    val json: Any = convertToCollection(res)
    mapper.valueToTree[JsonNode](json)
  }

  // The results returned by the query execution may contains instances of case classes that were generated dynamically
  // Here we convert these case classes to instances of maps with entries: fieldName -> fieldValue
  def convertToCollection(res: Any): Any = {
    if (res.isInstanceOf[Array[_]]) {
      res.asInstanceOf[Array[_]].map(convertToCollection(_))
    } else {
      res match {
        case imSet: ImmutableMultiset[_] =>
          val resultTyped: List[Any] = toScalaList(imSet)
          resultTyped.map(convertToCollection(_))
        case set: Set[_] =>
          set.map(convertToCollection(_))
        case bag: Bag[_] =>
          bag.map(convertToCollection(_))
        case list: List[_] =>
          list.map(convertToCollection(_))
        case m: Map[_, _] =>
          m.map({ case (k, v) => s"$k: ${convertToCollection(v)}" })
        case p: Product =>
          val fields: Array[Field] = p.getClass.getDeclaredFields
          fields.map(f => {
            f.setAccessible(true)
            f.getName -> convertToCollection(f.get(p))
          }
          ).toMap
        case _ => res
      }
    }
  }

  def convertToString(res: Any): String = {
    val result: String =
      if (res.isInstanceOf[Array[_]]) {
        resultsToString(res.asInstanceOf[Array[_]].toList)
      } else {
        res match {
          case bag: Bag[_] =>
            resultsToString(bag.toList)
          case imSet: ImmutableMultiset[_] =>
            val resultTyped: List[Any] = toScalaList(imSet)
            resultsToString(resultTyped)
          case set: Set[_] =>
            resultsToString(set.toList)
          case list: List[_] =>
            resultsToString(list)
          case _ => res.toString
        }
      }
    loggerQueries.info(s"Result:\n$result")
    result
  }

  private[this] def toScalaList[T](s: ImmutableMultiset[T]) = {
    JavaConversions.asScalaIterator[T](s.iterator).toList
  }

  private[this] def resultsToString[T](l: List[T]): String = {
    l.map(valueToString(_)).sorted.mkString("\n")
  }

  //  private[this] def resultsToString[T](s: Set[T]): String = {
  //    resultsToString(s.toList)
  //  }

  private[this] def valueToString[T](value: Any): String = {
    value match {
      case l: List[_] => l.map(valueToString(_)).sorted.mkString("[", ", ", "]")
      case s: Set[_] => s.map(valueToString(_)).toList.sorted.mkString("[", ", ", "]")
      case ms: ImmutableMultiset[_] => toScalaList(ms).map(valueToString(_)).sorted.mkString("[", ", ", "]")
      case m: Map[_, _] => m.map({ case (k, v) => s"$k: ${valueToString(v)}" }).toList.sorted.mkString("[", ", ", "]")
      case p: Product => valueToString(caseClassToMapOfStrings(p))
      case null => "null"
      case _ => value.toString
    }
  }

  def convertExpected(expected: String): String = {
    expected.replaceAll( """\n\s*""", "\n").trim
  }

  // WARN: There are many classes that implement Product, like List. If this method is called with something other
  // than a case class, it will likely fail.
  private[this] def caseClassToMapOfStrings(p: Product): Any = {
    val fields: Array[Field] = p.getClass.getDeclaredFields
    fields.map(f => {
      f.setAccessible(true)
      f.getName -> valueToString(f.get(p))
    }
    ).toMap
  }
}