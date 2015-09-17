package raw

import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import calculus.{Symbol, SymbolTable}

/** Types
  */
sealed abstract class Type extends RawNode {
  var nullable = false
}

/** Primitive Types
  */
case class BoolType() extends Type

case class StringType() extends Type

case class IntType() extends Type

case class FloatType() extends Type

case class PrimitiveType(sym: Symbol = SymbolTable.next()) extends VariableType

case class NumberType(sym: Symbol = SymbolTable.next()) extends VariableType

/** Record Type
  */
case class AttrType(idn: String, tipe: Type) extends RawNode

case class RecordType(atts: Seq[AttrType], name: Option[String]) extends Type {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Collection Type
  */
case class CollectionType(m: CollectionMonoid, innerType: Type) extends Type

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

/** User Type.
  * User-defined data type.
  */
case class UserType(sym: Symbol) extends Type

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

/** Type Scheme
  * TODO: Describe.
  */
case class TypeScheme(t: Type, vars: Set[Symbol]) extends Type


