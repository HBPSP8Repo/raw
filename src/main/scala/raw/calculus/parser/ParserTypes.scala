package raw.calculus.parser

/** ParserType
 */

abstract class ParserType

/** PrimitiveType
 */

sealed abstract class PrimitiveType extends ParserType
case object BoolType extends PrimitiveType
case object StringType extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType
case object FloatType extends NumberType
case object IntType extends NumberType

/** RecordType
 */

case class Attribute(name: String, parserType: ParserType)
case class RecordType(atts: List[Attribute]) extends ParserType

/** CollectionType
 */

sealed abstract class CollectionType(val innerType: ParserType) extends ParserType
case class SetType(t: ParserType) extends CollectionType(t)
case class BagType(t: ParserType) extends CollectionType(t)
case class ListType(t: ParserType) extends CollectionType(t)

/** VariableType
 *  
 *  Used for expressions whose type is unknown at creation time, e.g. Null.
 *  Overriding equals and hashCode so that VariableType instances are always different objects
 *  and don't try to unify with each other.
 */

case class VariableType() extends ParserType {
  override def equals(o: Any) = super.equals(o)
  override def hashCode = super.hashCode    
}

/** FunctionType
 *  
 *  Type for expressions 't2 -> t1', i.e. for functions.
 */
case class FunctionType(t1: ParserType, t2: ParserType) extends ParserType

/** ParserTypePrettyPrinter
 */
object ParserTypePrettyPrinter {
  def apply(t: ParserType): String = t match {
    case BoolType => "bool"
    case StringType => "string"
    case FloatType => "float"
    case IntType => "int"
    case RecordType(atts) => "record(" + atts.map(att => att.name + " = " + ParserTypePrettyPrinter(att.parserType)).mkString(", ") + ")" 
    case SetType(t) => "set(" + ParserTypePrettyPrinter(t) + ")"
    case BagType(t) => "bag(" + ParserTypePrettyPrinter(t) + ")"
    case ListType(t) => "list(" + ParserTypePrettyPrinter(t) + ")"
    case VariableType() => "unknown"
    case FunctionType(t1, t2) => "function(" + ParserTypePrettyPrinter(t) + " -> " + ParserTypePrettyPrinter(t2) + ")" 
  }
}