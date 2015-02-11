package raw

/** Types
  */
sealed abstract class Type extends RawNode {
  override def toString() = PrettyPrinter.pretty(PrettyPrinter.tipe(this))
}

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
