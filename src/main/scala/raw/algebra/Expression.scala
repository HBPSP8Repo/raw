package raw.algebra

import raw._

/** Expression for Algebra
 */

sealed abstract class Expression(val monoidType: MonoidType)

/** Constant
 */

sealed abstract class Constant(t: PrimitiveType) extends Expression(t)
case class BoolConst(v: Boolean) extends Constant(BoolType)
case class IntConst(v: Long) extends Constant(IntType)
case class FloatConst(v: Double) extends Constant(FloatType)
case class StringConst(v: String) extends Constant(StringType)

/** Argument
 */

case class Argument(t: MonoidType, id: Int) extends Expression(t)

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

/** MergeMonoid
 */

case class MergeMonoid(t: MonoidType, m: Monoid, e1: Expression, e2: Expression) extends Expression(t)

/** Unary Functions
 * 
 * TODO: Why aren't unary functions included in [1] (Fig. 2, page 12)?
 */

case class Not(e: Expression) extends Expression(BoolType)

case class FloatToInt(e: Expression) extends Expression(IntType)
case class FloatToString(e: Expression) extends Expression(StringType)
case class IntToFloat(e: Expression) extends Expression(FloatType)
case class IntToString(e: Expression) extends Expression(StringType)
case class StringToBool(e: Expression) extends Expression(BoolType)
case class StringToInt(e: Expression) extends Expression(IntType)
case class StringToFloat(e: Expression) extends Expression(FloatType)

/** ExpressionPrettyPrinter
 */

object ExpressionPrettyPrinter { 
  def apply(e: Expression, pre: String = ""): String = pre + (e match {
    case BoolConst(v) => if (v) "true" else "false"
    case IntConst(v) => v.toString()
    case FloatConst(v) => v.toString()
    case StringConst(v) => "\"" + v.toString() + "\""
    case Argument(_, id) => "<arg" + id.toString() + ">"
    case RecordProjection(_, e, name) => ExpressionPrettyPrinter(e) + "." + name
    case RecordConstruction(_, atts) => "( " + atts.map(att => att.name + " := " + ExpressionPrettyPrinter(att.e)).mkString(", ") + " )"
    case IfThenElse(_, e1, e2, e3) => "if " + ExpressionPrettyPrinter(e1) + " then " + ExpressionPrettyPrinter(e2) + " else " + ExpressionPrettyPrinter(e3)
    case BinaryOperation(_, op, e1, e2) => "( " + ExpressionPrettyPrinter(e1) + " " + BinaryOperatorPrettyPrinter(op) + " " + ExpressionPrettyPrinter(e2) + " )"
    case MergeMonoid(_, m, e1, e2) => "( " + ExpressionPrettyPrinter(e1) + " " + MonoidPrettyPrinter(m) + " " + ExpressionPrettyPrinter(e2) + " )"
    case Not(e) => "not(" + ExpressionPrettyPrinter(e) + ")"
    case FloatToInt(e) => "as_int(" + ExpressionPrettyPrinter(e) + ")"
    case FloatToString(e) => "as_string(" + ExpressionPrettyPrinter(e) + ")"
    case IntToFloat(e) => "as_float(" + ExpressionPrettyPrinter(e) + ")"
    case IntToString(e) => "as_string(" + ExpressionPrettyPrinter(e) + ")"
    case StringToBool(e) => "as_bool(" + ExpressionPrettyPrinter(e) + ")"
    case StringToInt(e) => "as_int(" + ExpressionPrettyPrinter(e) + ")"
    case StringToFloat(e) => "as_float(" + ExpressionPrettyPrinter(e) + ")"
  })
}