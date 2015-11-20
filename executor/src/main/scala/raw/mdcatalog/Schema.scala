package raw.mdcatalog

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util

import com.typesafe.scalalogging.StrictLogging
import raw._

import scala.collection.JavaConversions
import scala.collection.immutable.Seq
import scala.collection.mutable.HashMap
import scala.xml.{Elem, Node, XML}

case class CsvDataSourceProperties(hasHeader: Option[Boolean], fieldNames: Option[util.ArrayList[String]], delimiter: Option[Char])

object SchemaParser extends StrictLogging {

  def apply(path: Path, attributeOrder: Option[util.ArrayList[String]]): raw.Type = {
    apply(Files.newInputStream(path), attributeOrder)
  }

  def apply(is: InputStream, attributeOrder: Option[util.ArrayList[String]]): raw.Type = {
    try {
      val root: Elem = XML.load(is)
      val result = new RecursionWrapper(root, attributeOrder)
      //      val records =  result.records.toMap,
      result.rawType
    } finally {
      is.close()
    }
  }

  private[this] class RecursionWrapper(root: Elem, attributeOrder: Option[util.ArrayList[String]]) {
    // The record types found in the schema
    val rawType: Type = toRawType(root)
    var rootElementName: String = null

    private[this] def toRawType(elem: Elem): Type = {
      elem.label match {
        case "list" =>
          val innerType: Elem = getSingleChild(elem)
          CollectionType(ListMonoid(), toRawType(innerType))

        case "record" =>
          val fields: Seq[Elem] = getChildren(elem)
          val attrs: Seq[AttrType] = fields.map(f => parseAttrType(f))
          val orderedAttrs = orderFields(attrs)
          RecordType(Attributes(orderedAttrs))

        case "int" => IntType()
        case "boolean" => BoolType()
        case "float" => FloatType()
        case "string" => StringType()
        case _ => throw new RuntimeException("Unknown node: " + elem)
      }
    }

    private[this] def orderFields(attributes: Seq[AttrType]): Seq[AttrType] = {
      attributeOrder match {
        case None => attributes
        case Some(order: util.ArrayList[String]) => {
          logger.debug(s"Reordering attributes: $attributes, using order: $order")
          val attMap: Map[String, AttrType] = attributes.map(attr => attr.idn -> attr).toMap
          val orderIter = JavaConversions.asScalaIterator(order.iterator())
          val reordered = orderIter.map(attrName => attMap(attrName)).to[scala.collection.immutable.Seq]
          reordered
        }
      }
    }

    private[this] def getSingleChild(elem: Elem): Elem = {
      val elems = elem.child.filter(n => n.isInstanceOf[Elem]).asInstanceOf[Seq[Elem]]
      assert(elems.size == 1, "Expected single child element. Found: " + elems + ". node: " + elem)
      elems.head
    }

    private[this] def getChildren(elem: Elem): Seq[Elem] = {
      elem.child.filter(n => n.isInstanceOf[Elem]).asInstanceOf[Seq[Elem]]
    }

    private[this] def getAttributeText(elem: Elem, key: String): String = {
      val attr: Seq[Node] = elem.attributes(key)
      assert(attr.size == 1, "Unexpected number of nodes: " + attr)
      attr.head.text
    }

    private[this] def parseAttrType(elem: Elem): AttrType = {
      val idn = getAttributeText(elem, "name")
      val tipe = toRawType(getSingleChild(elem))
      AttrType(idn, tipe)
    }
  }

}
