package raw

/** Types
  */
sealed abstract class Type extends RawNode

/** Primitive Types
  */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType

case class StringType() extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType

case class FloatType() extends NumberType

case class IntType() extends NumberType

/** Product Type
  */

case class ProductType(tipes: Seq[Type]) extends Type

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType]) extends Type {
  def typeOf(attribute: String): Type = {
    val matches = atts.filter({ p => p match {
      case c: AttrType => c.idn == attribute
    }
    })
    return matches.head.tipe
  }
}

/** Collection Type
  */
case class CollectionType(m: CollectionMonoid, innerType: Type) extends Type

/** Class Type
  */
case class ClassType(idn: String) extends Type

/** Function Type `t2` -> `t1`
  */
case class FunType(val t1: Type, val t2: Type) extends Type

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
        case (CollectionType(m1, tA), CollectionType(m2, tB))  => m1 == m2 && compatible(tA, tB)
        case _                                                 => false
      })
  }

}