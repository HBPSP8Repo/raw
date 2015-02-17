package raw

/** Types
  */
sealed abstract class Type extends RawNode

/** Primitive Types
  */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType

case class StringType() extends PrimitiveType

case class NumberType() extends PrimitiveType

/** Product Type
  */

case class ProductType(tipes: Seq[Type]) extends Type

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType]) extends Type {
  def typeOf(attribute: String): Type = {
    val matches = atts.filter {
      case c: AttrType => c.idn == attribute
    }
    matches.head.tipe
  }
}

/** Collection Types
  */
abstract class CollectionType extends Type {
  def innerType: Type
}

case class BagType(innerType: Type) extends CollectionType
case class ListType(innerType: Type) extends CollectionType
case class SetType(innerType: Type) extends CollectionType

/** Class Type
  */
case class ClassType(idn: String) extends Type

/** Function Type `t2` -> `t1`
  */
case class FunType(t1: Type, t2: Type) extends Type

/** Unknown Type: any type will do
  */
case class UnknownType() extends Type

object Types {

  def compatible(t1: Type, t2: Type): Boolean = {
    (t1 == UnknownType()) ||
      (t2 == UnknownType()) ||
      (t1 == t2) ||
      ((t1, t2) match {
        case (ProductType(t1), ProductType(t2)) => {
          // TODO
          ???
        }
        case (RecordType(atts1), RecordType(atts2)) => {
          // Record types are compatible if they have at least one identifier in common, and if all identifiers have a
          // compatible type.
          val atts = atts1.flatMap{ case a1 @ AttrType(idn, _) => atts2.collect{ case a2 @ AttrType(`idn`, _) => (a1.tipe, a2.tipe) } }
          atts.nonEmpty && !atts.map{ case (a1, a2) => compatible(a1, a2) }.contains(false)
        }
        case (FunType(tA1, tA2), FunType(tB1, tB2))            => compatible(tA1, tB1) && compatible(tA2, tB2)
        case (BagType(tA), BagType(tB))                        => compatible(tA, tB)
        case (ListType(tA), ListType(tB))                      => compatible(tA, tB)
        case (SetType(tA), SetType(tB))                        => compatible(tA, tB)
        case _                                                 => false
      })
  }

}