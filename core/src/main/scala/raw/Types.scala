package raw

import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import calculus.{Symbol, SymbolTable}

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
sealed abstract class CollectionType extends Type {
  def innerType: Type
}

case class BagType(innerType: Type) extends CollectionType
case class ListType(innerType: Type) extends CollectionType
case class SetType(innerType: Type) extends CollectionType

/** Function Type `t2` -> `t1`
  */
case class FunType(t1: Type, t2: Type) extends Type

/** Any Type
  * The top type.
  */
case class AnyType() extends Type

/** Nothing Type
  * The bottom type.
  */
case class NothingType() extends Type

/** Abstract class representing all types that vary: TypeVariable, constrainted/partial types, etc.
  */

sealed abstract class VariableType extends Type {
  def sym: Symbol
}

/** Type Variable
  */
case class TypeVariable(sym: Symbol = SymbolTable.next()) extends VariableType

/** Constraint Record Type
  */
case class ConstraintRecordType(atts: Set[AttrType], sym: Symbol = SymbolTable.next()) extends VariableType {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Constraint Collection Type
  */
case class ConstraintCollectionType(innerType: Type, commutative: Option[Boolean], idempotent: Option[Boolean], sym: Symbol = SymbolTable.next()) extends VariableType

