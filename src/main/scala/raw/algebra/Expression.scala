package raw.algebra

import raw._

/** Expression for Algebra
 */

sealed abstract class Expression(val monoidType: MonoidType)

/** Null
 */

case object Null extends Expression(VariableType())

/** Constant
 */

sealed abstract class Constant(t: MonoidType) extends Expression(t)
case class BoolConst(v: Boolean) extends Constant(BoolType)
case class IntConst(v: Long) extends Constant(IntType)
case class FloatConst(v: Double) extends Constant(FloatType)
case class StringConst(v: String) extends Constant(StringType)

/** Variable
 */

case class Variable(v: calculus.canonical.Variable) extends Expression(v.monoidType)

/** ClassExtent
 */
 
case class ClassExtent(t: MonoidType, id: String) extends Expression(t)

/** RecordProjection
 */

case class RecordProjection(t: MonoidType, e: Expression, name: String) extends Expression(t)

/** RecordConstruction
 */

case class AttributeConstruction(name: String, e: Expression)
case class RecordConstruction(t: MonoidType, atts: List[AttributeConstruction]) extends Expression(t)

/** IfThenElse
 */

case class IfThenElse(t: MonoidType, e1: Expression, e2: Expression, e3: Expression) extends Expression(t)

/** BinaryOperation
 */

case class BinaryOperation(t: MonoidType, op: BinaryOperator, e1: Expression, e2: Expression) extends Expression(t)

/** Zeroes for Collection Monoids
 */

case class EmptySet extends Expression(VariableType())
case class EmptyBag extends Expression(VariableType())
case class EmptyList extends Expression(VariableType())

/** ConsCollectionMonoid
 */

case class ConsCollectionMonoid(t: MonoidType, m: CollectionMonoid, e: Expression) extends Expression(t)

/** MergeMonoid
 */

case class MergeMonoid(t: MonoidType, m: Monoid, e1: Expression, e2: Expression) extends Expression(t)

/** Unary Functions
 * 
 * TODO: Why aren't unary functions included in [1] (Fig. 2, page 12)?
 */

case class Not(e: Expression) extends Expression(BoolType)

/** ExpressionPrettyPrinter
 */

object ExpressionPrettyPrinter { 
  def apply(e: Expression, pre: String = ""): String = pre + (e match {
    case Null => "null"
    case BoolConst(v) => if (v) "true" else "false"
    case IntConst(v) => v.toString()
    case FloatConst(v) => v.toString()
    case StringConst(v) => "\"" + v.toString() + "\""
    case Variable(_) => "v" + e.hashCode().toString()
    case ClassExtent(_, id) => "`" + id + "`"
    case RecordProjection(_, e, name) => ExpressionPrettyPrinter(e) + "." + name
    case RecordConstruction(_, atts) => "( " + atts.map(att => att.name + " := " + ExpressionPrettyPrinter(att.e)).mkString(", ") + " )"
    case IfThenElse(_, e1, e2, e3) => "if " + ExpressionPrettyPrinter(e1) + " then " + ExpressionPrettyPrinter(e2) + " else " + ExpressionPrettyPrinter(e3)
    case BinaryOperation(_, op, e1, e2) => "( " + ExpressionPrettyPrinter(e1) + " " + BinaryOperatorPrettyPrinter(op) + " " + ExpressionPrettyPrinter(e2) + " )"
    case EmptySet() => "{}"
    case EmptyBag() => "bag{}"
    case EmptyList() => "[]"
    case ConsCollectionMonoid(_, SetMonoid(), e) => "{ " + ExpressionPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, BagMonoid(), e) => "bag{ " + ExpressionPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, ListMonoid(), e) => "[ " + ExpressionPrettyPrinter(e) + " ]"
    case MergeMonoid(_, m, e1, e2) => "( " + ExpressionPrettyPrinter(e1) + " " + MonoidPrettyPrinter(m) + " " + ExpressionPrettyPrinter(e2) + " )"
    case Not(e) => "not(" + ExpressionPrettyPrinter(e) + ")"
  })
}