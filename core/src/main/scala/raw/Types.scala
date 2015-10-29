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

case class DateTimeType() extends Type

case class DateType() extends Type

case class TimeType() extends Type

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

sealed abstract class VariableAttributes extends RecordAttributes {
  def sym: Symbol
}

/** Attributes (fixed-size, well known)
 */

case class Attributes(atts: Seq[AttrType]) extends RecordAttributes {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Attributes Variable (a set of some known attributes)
  */

case class AttributesVariable(atts: Set[AttrType], sym: Symbol = SymbolTable.next()) extends VariableAttributes {
  def getType(idn: String): Option[Type] = atts.filter { case att => att.idn == idn }.map(_.tipe).headOption
}

/** Concatenation of attributes.
 */

case class ConcatAttributes(sym: Symbol = SymbolTable.next()) extends VariableAttributes {
  // TODO: Fix hierarchy: remove atts and getType(idn: String)
  def getType(idn: String) = ???
  def atts = ???
}

//
//// TODO: Add pattern idn to each sequence of atts
//case class ConcatAttributes(atts: Seq[AttrType] = Seq(), sym: Symbol = SymbolTable.next()) extends RecordAttributes {
//  def getType(idn: String) = ???
//}
//
//case class ConcatAttrSeq(prefix: String, t: Type)
//
//case class ConcatAttributes(catts: Seq[ConcatAttrSeq], sym: Symbol = SymbolTable.next()) extends RecordAttributes {
//  // TODO: Fix hierarchy
//  def getType(idn: String) = ???
//  def atts = ???
//}
//
///** Variable and Concatenated attributes.
//  */
//
//case class VarConcatAttributes(varSets: Set[AttributesVariable], concatSets: Set[ConcatAttributes], sym: Symbol = SymbolTable.next()) extends RecordAttributes {
//  def getType(idn: String) = ???
//  def atts = ???
//}

// concat attributes contained in another
// suppose the inner one is resolved (to a fixed size thing)
// then the outer one may do some progress as well
// ok.
// that's where my other impl may help
// but what is the root then?
// well if concat attributes itself had nothing, could be a new concat attributes just for the sake of it
// ok, gotcha

/** Record Type
  */

case class RecordType(recAtts: RecordAttributes) extends Type {
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

/** Regex Type.
  */
case class RegexType() extends Type

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
