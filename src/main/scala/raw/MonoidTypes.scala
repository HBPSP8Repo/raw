package raw

/**
 * MonoidType
 */
sealed abstract class MonoidType

/**
 * PrimitiveType
 */
sealed abstract class PrimitiveType extends MonoidType
case object BoolType extends PrimitiveType
case object StringType extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType
case object FloatType extends NumberType
case object IntType extends NumberType

/**
 * RecordType
 */
case class Attribute( name: String, monoidType: MonoidType )
case class RecordType( atts: List[Attribute] ) extends MonoidType

/**
 * CollectionType
 */
sealed abstract class CollectionType( val monoidType: MonoidType ) extends MonoidType
case class SetType( t: MonoidType ) extends CollectionType( t )
case class BagType( t: MonoidType ) extends CollectionType( t )
case class ListType( t: MonoidType ) extends CollectionType( t )

/**
 * VariableType
 *
 * VariableType should appear rarely in practice.
 * One such case is a comprehension that gets transformed into an EmptySet, whose type is VariableType.
 */
case object VariableType extends MonoidType

/**
 * MonoidTypePrettyPrinter
 */
object MonoidTypePrettyPrinter {
  def apply( t: MonoidType ): String = t match {
    case BoolType           => "bool"
    case StringType         => "string"
    case FloatType          => "float"
    case IntType            => "int"
    case RecordType( atts ) => "record(" + atts.map( att => att.name + " = " + MonoidTypePrettyPrinter( att.monoidType ) ).mkString( ", " ) + ")"
    case SetType( t )       => "set(" + MonoidTypePrettyPrinter( t ) + ")"
    case BagType( t )       => "bag(" + MonoidTypePrettyPrinter( t ) + ")"
    case ListType( t )      => "list(" + MonoidTypePrettyPrinter( t ) + ")"
    case VariableType       => "unknown"
  }
}