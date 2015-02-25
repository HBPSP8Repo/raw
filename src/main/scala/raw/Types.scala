package raw

import scala.collection.immutable.Seq

/** Types
  */
sealed abstract class Type extends RawNode

/** Primitive Types
  */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType

case class StringType() extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType

case class IntType() extends NumberType

case class FloatType() extends NumberType

/** Product Type
  */
case class ProductType(tipes: Seq[Type]) extends Type

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType]) extends Type {
  def idns = atts.map(_.idn)

  // TODO: Reuse this impl elsewhere
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

/** Type Variable
  */
case class TypeVariable(v: Variable) extends Type

class Variable()

/** Any Type
  * The top type.
  */
case class AnyType() extends Type

/** Nothing Type
  * The bottom type.
  */
case class NothingType() extends Type
