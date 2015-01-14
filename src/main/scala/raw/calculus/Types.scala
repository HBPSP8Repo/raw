package raw.calculus

import org.kiama.util.TreeNode

/** Types
 */
sealed abstract class Type extends TreeNode

/** Primitive Types
 */
sealed abstract class PrimitiveType extends Type

case class BoolType() extends PrimitiveType {
  override def toString() = "bool"
}
case class StringType() extends PrimitiveType {
  override def toString() = "string"
}

sealed abstract class NumberType extends PrimitiveType

case class FloatType() extends NumberType {
  override def toString() = "float"
}
case class IntType() extends NumberType {
  override def toString() = "int"
}

/** Record Type
 */
case class AttrType(idn: String, tipe: Type) {
  override def toString() = s"$idn = $tipe"
}
case class RecordType(atts: Seq[AttrType]) extends Type {
  override def toString() = s"record(${atts.map(_.toString()).mkString(", ")})"
}

/** Collection Type
 */
case class CollectionType(m: CollectionMonoid, innerType: Type) extends Type {
  override def toString() = s"$m($innerType)"
}

/** Class Type
 */
case class ClassType(idn: String) extends Type {
  override def toString() = s"$idn"
}

/** Function Type `t2` -> `t1`
 */
case class FunType(val t1: Type, val t2: Type) extends Type {
  override def toString() = s"function($t1 -> $t2)"
}

/** No Type: for untyped expressions
 */
case class NoType() extends Type {
  override def toString() = "none"
}

/** Unknown Type: any type will do
 */
case class UnknownType() extends Type {
  override def toString() = "unknown"
}
