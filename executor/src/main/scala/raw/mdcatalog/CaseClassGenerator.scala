package raw.mdcatalog

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import raw._

import scala.collection.immutable.Seq
import scala.collection.mutable


/**
  *
  * @param caseClasses Map from class name to class definition (scala source code)
  * @param typeDeclaration
  */
case class ParsedSchema(caseClasses: Map[String, String], typeDeclaration: String)

object CaseClassGenerator extends StrictLogging {
  // map of unique record names
  val recordNames = scala.collection.mutable.HashMap[RecordType, String]()
  val c = new AtomicInteger(0)

  def recordName(r: RecordType) = {
    if (!recordNames.contains(r)) {
      recordNames.put(r, s"__UserRecord${c.getAndIncrement()}")
    }
    recordNames(r)
  }

  def apply(schema: DataSource): ParsedSchema = {
    val typeGenerator = new ScalaTypeGenerator(schema.tipe)
    ParsedSchema(typeGenerator.caseClassesSym.toMap, typeGenerator.typeDeclaration)
  }

  class ScalaTypeGenerator(rawType: raw.Type) {
    val typeDeclaration: String = buildScalaDeclaration(rawType)
    val caseClassesSym = new mutable.HashMap[String, String]()
    defineCaseClasses(rawType)
//    logger.debug("Case classes: " + caseClassesSym)
    val caseClassesSource = caseClassesSym.values.mkString("\n")

    private[this] def defineCaseClass(r: RecordType): String = {
      r match {
        case RecordType(Attributes(atts)) =>
          val idn = recordName(r)
          caseClassesSym.get(idn) match {
            case Some(src) =>
              logger.info(s"case class $idn already defined")
              idn

            case None =>
              //          logger.info(s"Defining case class: $idn. attr: ${r.atts}")
              val attributes = buildAttributeList(atts)
              val src = s"""case class $idn( $attributes )"""
              caseClassesSym.put(idn, src)
              idn
          }
        case _ => throw new UnsupportedOperationException("Cannot define case class for partial records")
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

    private[this] def defineCaseClasses(t: Type): Unit = {
      t match {
        case r@RecordType(atts) => defineCaseClass(r)
        case CollectionType(BagMonoid(), innerType) => defineCaseClasses(innerType)
        case CollectionType(ListMonoid(), innerType) => defineCaseClasses(innerType)
        case CollectionType(SetMonoid(), innerType) => defineCaseClasses(innerType)
        case _ => // Ignore all types which don't contain other inner types.
      }
    }

    private[this] def buildScalaDeclaration(t: Type): String = {
      val code: String = t match {
        case _: BoolType => "Boolean"
        case _: StringType => "String"
        case _: IntType => "Int"
        case _: FloatType => "Float"
        case r: RecordType => recordName(r)
        case CollectionType(BagMonoid(), innerType) => s"Seq[${buildScalaDeclaration(innerType)}]"
        case CollectionType(ListMonoid(), innerType) => s"Seq[${buildScalaDeclaration(innerType)}]"
        case CollectionType(SetMonoid(), innerType) => s"Set[${buildScalaDeclaration(innerType)}]"
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
}
