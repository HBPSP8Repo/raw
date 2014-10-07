package raw.logical.calculus

import raw._
import raw.logical._

/**
 * Expressions for Calculus
 */
sealed abstract class Expression
sealed abstract class TypedExpression(val monoidType: MonoidType) extends Expression
sealed abstract class UntypedExpression extends Expression

/**
 * Null
 */
case object Null extends TypedExpression(VariableType)

/**
 * Constant
 */
sealed abstract class Constant(t: MonoidType) extends TypedExpression(t)
case class BoolConst(v: Boolean) extends Constant(BoolType)
case class IntConst(v: Long) extends Constant(IntType)
case class FloatConst(v: Double) extends Constant(FloatType)
case class StringConst(v: String) extends Constant(StringType)

/**
 * Variable
 * 
 * The uniqueId is a string used to distinguish variables with the same name in different scopes.
 */
case class Variable(t: MonoidType, name: String, uniqueId: String) extends TypedExpression(t)

/**
 * RecordProjection
 */
case class RecordProjection(t: MonoidType, e: TypedExpression, name: String) extends TypedExpression(t)

/**
 * RecordConstruction
 */
case class AttributeConstruction(name: String, e: TypedExpression)
case class RecordConstruction(t: MonoidType, atts: List[AttributeConstruction]) extends TypedExpression(t)

/**
 * IfThenElse
 */
case class IfThenElse(t: MonoidType, e1: TypedExpression, e2: TypedExpression, e3: TypedExpression) extends TypedExpression(t)

/**
 * BinaryOperation
 */
case class BinaryOperation(t: MonoidType, op: BinaryOperator, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/**
 * Zeroes for Collection Monoids
 */
case object EmptySet extends TypedExpression(VariableType)
case object EmptyBag extends TypedExpression(VariableType)
case object EmptyList extends TypedExpression(VariableType)

/**
 * ConsCollectionMonoid
 */
case class ConsCollectionMonoid(t: MonoidType, m: CollectionMonoid, e: TypedExpression) extends TypedExpression(t)

/**
 * MergeMonoid
 */
case class MergeMonoid(t: MonoidType, m: Monoid, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/**
 * Comprehension
 */
case class Comprehension(t: MonoidType, m: Monoid, e: TypedExpression, gs: List[Generator], pred: TypedExpression) extends TypedExpression(t)

/**
 * Path (Generator)
 */
abstract class Path(val monoidType: MonoidType)
case class VariablePath(v: Variable) extends Path(v.monoidType)
case class InnerPath(p: Path, name: String) extends Path(p.monoidType match { 
  case RecordType(atts) => {
    atts.find(_.name == name) match {
       case Some(att) => att.monoidType
       case _ => throw RawInternalException("badly formed inner path")
    }
  }
  case _ => throw RawInternalException("unexpected type in inner path")
})

/**
 * Generator
 */
case class Generator(v: Variable, p: Path) extends UntypedExpression

/**
 * Unary Functions
 */
case class Not(e: TypedExpression) extends TypedExpression(BoolType)
case class FloatToInt(e: TypedExpression) extends TypedExpression(IntType)
case class FloatToString(e: TypedExpression) extends TypedExpression(StringType)
case class IntToFloat(e: TypedExpression) extends TypedExpression(FloatType)
case class IntToString(e: TypedExpression) extends TypedExpression(StringType)
case class StringToBool(e: TypedExpression) extends TypedExpression(BoolType)
case class StringToInt(e: TypedExpression) extends TypedExpression(IntType)
case class StringToFloat(e: TypedExpression) extends TypedExpression(FloatType)

/**
 * PathPrettyPrinter
 */
object PathPrettyPrinter {
  def apply(p: Path): String = p match {
    case VariablePath(v) => CalculusPrettyPrinter(v)
    case InnerPath(p, name) => PathPrettyPrinter(p) + "." + name
  }
}

/**
 * CalculusPrettyPrinter
 */
object CalculusPrettyPrinter { 
  def apply(e: Expression, pre: String = ""): String = pre + (e match {
    case Null => "null"
    case BoolConst(v) => if (v) "true" else "false"
    case IntConst(v) => v.toString()
    case FloatConst(v) => v.toString()
    case StringConst(v) => "\"" + v.toString() + "\""
    case Variable(_, name, _) => name
    case RecordProjection(_, e, name) => CalculusPrettyPrinter(e) + "." + name
    case RecordConstruction(_, atts) => "( " + atts.map(att => att.name + " := " + CalculusPrettyPrinter(att.e)).mkString(", ") + " )"
    case IfThenElse(_, e1, e2, e3) => "if " + CalculusPrettyPrinter(e1) + " then " + CalculusPrettyPrinter(e2) + " else " + CalculusPrettyPrinter(e3)
    case BinaryOperation(_, op, e1, e2) => "( " + CalculusPrettyPrinter(e1) + " " + BinaryOperatorPrettyPrinter(op) + " " + CalculusPrettyPrinter(e2) + " )"
    case EmptySet => "{}"
    case EmptyBag => "bag{}"
    case EmptyList => "[]"
    case ConsCollectionMonoid(_, SetMonoid, e) => "{ " + CalculusPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, BagMonoid, e) => "bag{ " + CalculusPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, ListMonoid, e) => "[ " + CalculusPrettyPrinter(e) + " ]"
    case MergeMonoid(_, m, e1, e2) => "( " + CalculusPrettyPrinter(e1) + " merge " + MonoidPrettyPrinter(m) + " " + CalculusPrettyPrinter(e2) + " )"
    case Comprehension(_, m, e, qs, pred) => 
      "for ( " + (if (!qs.isEmpty) (qs.map(CalculusPrettyPrinter(_)).mkString(", ") + ", ") else "") + CalculusPrettyPrinter(pred) + " ) yield " + MonoidPrettyPrinter(m) + " " + CalculusPrettyPrinter(e)
    case Generator(v, p) => CalculusPrettyPrinter(v) + " <- " + PathPrettyPrinter(p)
    case Not(e) => "not(" + CalculusPrettyPrinter(e) + ")"
    case FloatToInt(e) => "as_int(" + CalculusPrettyPrinter(e) + ")"
    case FloatToString(e) => "as_string(" + CalculusPrettyPrinter(e) + ")"
    case IntToFloat(e) => "as_float(" + CalculusPrettyPrinter(e) + ")"
    case IntToString(e) => "as_string(" + CalculusPrettyPrinter(e) + ")"
    case StringToBool(e) => "as_bool(" + CalculusPrettyPrinter(e) + ")"
    case StringToInt(e) => "as_int(" + CalculusPrettyPrinter(e) + ")"
    case StringToFloat(e) => "as_float(" + CalculusPrettyPrinter(e) + ")"
  })
}