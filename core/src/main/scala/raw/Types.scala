package raw

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

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

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType], name: Option[String]) extends Type {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Collection Types
  */
abstract class CollectionType extends Type {
  def innerType: Type
}

case class BagType(innerType: Type) extends CollectionType
case class ListType(innerType: Type) extends CollectionType
case class SetType(innerType: Type) extends CollectionType

/** User Type
  */
case class UserType(idn: String) extends Type

/** Function Type `t2` -> `t1`
  */
case class FunType(t1: Type, t2: Type) extends Type

/** Type Variable
  */
case class TypeVariable(v: Variable) extends Type

class Variable()

/** Constraint Record Type
  */
case class ConstraintRecordType(atts: Set[AttrType]) extends Type {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Constraint Collection Type
  */
case class ConstraintCollectionType(innerType: Type, commutative: Option[Boolean], idempotent: Option[Boolean]) extends Type

/** Any Type
  * The top type.
  */
case class AnyType() extends Type

/** Nothing Type
  * The bottom type.
  */
case class NothingType() extends Type
