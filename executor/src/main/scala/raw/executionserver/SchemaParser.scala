package raw.executionserver

import java.io.Reader
import java.net.URL

import com.typesafe.scalalogging.StrictLogging
import raw._
import raw.datasets.AccessPath
import raw.perf.QueryCompilerClient

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.xml.{Elem, Node, XML}

case class ParsedSchema(caseClasses:Map[String, String], typeDeclaration:String)

object SchemaParser extends StrictLogging {
  def apply(schema: Reader): ParsedSchema = {
    val xml: Elem = XML.load(schema)
    logger.info("XML: " + xml)
    val parseResult = new SchemaAsRawType(xml)    
    logger.info("RawType: " + parseResult.rawType)
    logger.info("Case classes: " + parseResult.records)
    val typeGenerator = new ScalaTypeGenerator(parseResult)    
    ParsedSchema(typeGenerator.caseClassesSym.toMap, typeGenerator.typeDeclaration)    
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

  class ScalaTypeGenerator(rawType: SchemaAsRawType) {
    val typeDeclaration: String = buildScalaDeclaration(rawType.rawType)

    val caseClassesSym = new mutable.HashMap[String, String]()
    defineCaseClasses(rawType.rawType)

    val caseClassesSource = caseClassesSym.values.mkString("\n")

    private[this] def defineCaseClass(r: RecordType): String = {
      val idn = r.name.get
      caseClassesSym.get(idn) match {
        case Some(src) =>
          logger.info(s"case class $idn already defined")
          idn

        case None =>
          logger.info(s"Defining case class: $idn. attr: ${r.atts}")
          val attributes = r.atts.map(att => {
            val symbol = att.tipe match {
              case rt: RecordType =>
                defineCaseClass(rt)

              case _ => {
                defineCaseClasses(att.tipe)
                buildScalaDeclaration(att.tipe)
              }
            }

            s"${att.idn}:${symbol}"
          }).mkString(", ")

          val src = s"""case class $idn( $attributes )"""
          logger.info(s"Source code: $src")
          caseClassesSym.put(idn, src)
          idn
      }
    }

    private[this] def defineCaseClasses(t: raw.Type): Unit = {
      logger.info("Case class generation: " + t)
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
      //    logger.info(s"Type: $tt")
      code
    }
  }

  private class SchemaAsRawType(root: Elem) {
    val records = new HashMap[String, RecordType]()
    val rawType: raw.Type = toRawType(root)

    private[this] def parseAttrType(elem: Elem): AttrType = {
      val idn = getAttributeText(elem, "name")
      val tipe = toRawType(getSingleChild(elem))
      AttrType(idn, tipe)
    }

    private[this] def toRawType(elem: Elem): raw.Type = {
      elem.label match {
        case "list" =>
          val innerType: Elem = getSingleChild(elem)
          ListType(toRawType(innerType))

        case "record" =>
          val name: String = getAttributeText(elem, "name")
          logger.info("Name: " + name + ", case classes: " + records)
          records.get(name) match {
            case Some(sym) => sym
            case None =>
              val fields: Seq[Elem] = getChildren(elem)
              val attrs: Seq[AttrType] = fields.map(f => parseAttrType(f))
              val record = RecordType(attrs, Some(name))
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
  }

}
