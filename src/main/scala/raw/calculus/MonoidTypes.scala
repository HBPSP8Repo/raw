package raw.calculus

/** MonoidType
 */

abstract class MonoidType

/** PrimitiveType
 */

sealed abstract class PrimitiveType extends MonoidType
case object BoolType extends PrimitiveType
case object StringType extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType
case object FloatType extends NumberType
case object IntType extends NumberType

/** RecordType
 */

case class Attribute(name: String, monoidType: MonoidType)
case class RecordType(atts: List[Attribute]) extends MonoidType

/** CollectionType
 */

sealed abstract class CollectionType(val monoidType: MonoidType) extends MonoidType
case class SetType(t: MonoidType) extends CollectionType(t)
case class BagType(t: MonoidType) extends CollectionType(t)
case class ListType(t: MonoidType) extends CollectionType(t)

/** VariableType
 *  
 *  Used for expressions whose type is unknown at creation time, e.g. Null
 */
case class VariableType extends MonoidType {
  override def equals(o: Any) = super.equals(o)
  override def hashCode = super.hashCode    
}

/** FunctionType
 *  
 *  Type for expressions 't2 -> t1', i.e. for functions
 */
case class FunctionType(t1: MonoidType, t2: MonoidType) extends MonoidType
