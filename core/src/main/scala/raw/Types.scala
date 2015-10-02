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

/**
 *
 */

case class AttrType(idn: String, tipe: Type) extends RawNode

sealed abstract class RecordAttributes extends RawNode {
  def atts: Iterable[AttrType]

  def getType(idn: String): Option[Type]
}

case class Attributes(atts: Seq[AttrType]) extends RecordAttributes {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Attributes Variable
  */

case class AttributesVariable(atts: Set[AttrType], sym: Symbol = SymbolTable.next()) extends RecordAttributes {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Record Type
  */

case class RecordType(recAtts: RecordAttributes, name: Option[String]) extends Type {
  def getType(idn: String): Option[Type] = recAtts.getType(idn)
}

/** Pattern Type
  */

case class PatternAttrType(tipe: Type) extends RawNode

case class PatternType(atts: Seq[PatternAttrType]) extends Type

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

/** Abstract class representing all types that vary: TypeVariable, NumberType, ...
  */

sealed abstract class VariableType extends Type {
  def sym: Symbol
}

/** Type Variable
  */
case class TypeVariable(sym: Symbol = SymbolTable.next()) extends VariableType

/** Type Scheme
  * TODO: Describe.
  */
case class TypeScheme(t: Type, typeSyms: Set[Symbol], monoidSyms: Set[Symbol], attSyms: Set[Symbol]) extends Type


