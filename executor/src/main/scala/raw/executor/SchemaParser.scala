package raw.executor

import java.nio.file.Files

import com.typesafe.scalalogging.StrictLogging
import raw._

import scala.collection.immutable.Seq
import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.HashMap
import scala.xml.{Elem, Node, XML}

/**
 *
 * @param caseClasses Map from class name to class definition (scala source code)
 * @param typeDeclaration
 */
case class ParsedSchema(caseClasses: Map[String, String], typeDeclaration: String)

object SchemaParser extends StrictLogging {
  def apply(schema: RawSchema): ParsedSchema = {
    val asRawType: SchemaAsRawType = XmlToRawType(schema)
    logger.info("Parsed schema: " + asRawType.caseClasses)
    val typeGenerator = new ScalaTypeGenerator(asRawType)
    ParsedSchema(typeGenerator.caseClassesSym.toMap, typeGenerator.typeDeclaration)
  }

  class ScalaTypeGenerator(rawType: SchemaAsRawType) {
    val typeDeclaration: String = buildScalaDeclaration(rawType.typeDeclaration)
    val caseClassesSym = new mutable.HashMap[String, String]()
    defineCaseClasses(rawType.typeDeclaration)
    logger.info("Case classes: " + caseClassesSym)
    val caseClassesSource = caseClassesSym.values.mkString("\n")

    private[this] def defineCaseClass(r: RecordType): String = {
      val idn = r.name.get
      caseClassesSym.get(idn) match {
        case Some(src) =>
          logger.info(s"case class $idn already defined")
          idn

        case None =>
          //          logger.info(s"Defining case class: $idn. attr: ${r.atts}")
          val attributes = buildAttributeList(r.atts)
          val src = s"""case class $idn( $attributes )"""
          caseClassesSym.put(idn, src)
          idn
      }
    }

    private[this] def buildAttributeList(atts: Seq[AttrType]): String = {
      atts.map(att => {
        val idn = att.idn
        val symbol = att.tipe match {
          case rt: RecordType => defineCaseClass(rt)
          case _ => {
            defineCaseClasses(att.tipe)
            buildScalaDeclaration(att.tipe)
          }
        }

        s"${idn}:${symbol}"
      }).mkString(", ")
    }

    private[this] def defineCaseClasses(t: raw.Type): Unit = {
      t match {
        case r@RecordType(atts, Some(idn)) => defineCaseClass(r)
        case BagType(innerType) => defineCaseClasses(innerType)
        case ListType(innerType) => defineCaseClasses(innerType)
        case SetType(innerType) => defineCaseClasses(innerType)
        case _ => // Ignore all types which don't contain other inner types.
      }
    }

    private[this] def buildScalaDeclaration(t: raw.Type): String = {
      val code: String = t match {
        case _: BoolType => "Boolean"
        case _: StringType => "String"
        case _: IntType => "Int"
        case _: FloatType => "Float"
        case r@RecordType(atts, Some(idn)) => idn
        case BagType(innerType) => s"Seq[${buildScalaDeclaration(innerType)}]"
        case ListType(innerType) => s"Seq[${buildScalaDeclaration(innerType)}]"
        case SetType(innerType) => s"Set[${buildScalaDeclaration(innerType)}]"
        case UserType(idn) => ???
        case TypeVariable(v) => ???
        case FunType(t1, t2) => ???
        case _: AnyType => ???
        case _: NothingType => ???
        case _: CollectionType => ???
      }
      code
    }
  }

  case class SchemaAsRawType(caseClasses: Map[String, RecordType], typeDeclaration: raw.Type)

  private object XmlToRawType {
    def apply(schema: RawSchema): SchemaAsRawType = {
      val schemaXML = Files.newBufferedReader(schema.schemaFile)
      val root: Elem = XML.load(schemaXML)
      val result = new RecursionWrapper(root, schema)
      SchemaAsRawType(result.records.toMap, result.rawType)
    }

    private[this] class RecursionWrapper(root: Elem, schema: RawSchema) {
      val records = new HashMap[String, RecordType]()
      val rawType: raw.Type = toRawType(root)
      var rootElementName: String = null

      private[this] def toRawType(elem: Elem): raw.Type = {
        elem.label match {
          case "list" =>
            val innerType: Elem = getSingleChild(elem)
            ListType(toRawType(innerType))

          case "record" =>
            val name: String = getAttributeText(elem, "name")
            if (rootElementName == null) {
              rootElementName = name
            }
            records.get(name) match {
              case Some(sym) => sym
              case None =>
                val fields: Seq[Elem] = getChildren(elem)
                val attrs: Seq[AttrType] = fields.map(f => parseAttrType(f))
                logger.info("Attributes: " + attrs)
                val orderedAttrs = orderFields(name, attrs)
                val record = RecordType(orderedAttrs, Some(name))
                records.put(name, record)
                record
            }

          case "int" => IntType()
          case "boolean" => BoolType()
          case "float" => FloatType()
          case "string" => StringType()
          case _ => throw new RuntimeException("Unknown node: " + elem)
        }
      }

      private[this] def orderFields(name: String, attributes: Seq[AttrType]): Seq[AttrType] = {
        schema.properties.fieldNames() match {
          case None => attributes
          case Some(order) => {
            assert(name == rootElementName, "Unexpected field names metadata on nested schema. Processing element: " + name + ", rootelement: " + rootElementName + ", Schema: " + schema)
            logger.info("Reordering: " + attributes + ", using order: " + order)
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
}
