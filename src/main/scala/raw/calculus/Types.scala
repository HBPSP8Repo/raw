package raw.calculus

import org.kiama.util.TreeNode

/** Types
 */
sealed abstract class Type extends TreeNode

/** Primitive Types
 */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType
case class StringType() extends PrimitiveType

sealed abstract class NumberType extends PrimitiveType

case class FloatType() extends NumberType
case class IntType() extends NumberType

/** Record Type
 */
case class AttrType(idn: String, tipe: Type)
case class RecordType(atts: Seq[AttrType]) extends Type

/** Collection Type
 */
case class CollectionType(m: CollectionMonoid, innerType: Type) extends Type

/** Class Type
 */
case class ClassType(idn: String) extends Type

/** Function Type `t2` -> `t1`
 */
case class FunType(val t1: Type, val t2: Type) extends Type

/** No Type: for untyped expressions
 */
case class NoType() extends Type

/** Unknown Type: any type will do
 */
case class UnknownType() extends Type