/**
 * Calculus for the Parser.
 */
package raw.logical.calculus.parser

import scala.util.parsing.input.Positional

import raw._

/**
 * Expressions
 */
sealed abstract class Expression extends Positional

sealed abstract class TypedExpression(val parserType: ParserType) extends Expression

sealed abstract class UntypedExpression extends Expression

/**
 * Null
 *  
 *  Null is a class and not an object so that it holds its position.
 *  (Null inherits from TypedExpression -> Expression -> Positional).
 */
case class Null() extends TypedExpression(VariableType())

/**
 * Constant
 */
sealed abstract class Constant(t: ParserType) extends TypedExpression(t)
case class BoolConst(v: Boolean) extends Constant(BoolType)
case class IntConst(v: Long) extends Constant(IntType)
case class FloatConst(v: Double) extends Constant(FloatType)
case class StringConst(v: String) extends Constant(StringType)

/**
 * Variable
 *  
 * Overridding equals and hashCode so that every instance of the case class is unique.
 * Otherwise, all variables of the same type and name would be the same object,
 * even if defined across nested scope comprehensions.
 * 
 */
case class Variable(t: ParserType, name: String) extends TypedExpression(t) {
  override def equals(o: Any) = super.equals(o)
  override def hashCode = super.hashCode
}

/**
 * RecordProjection
 */
case class RecordProjection(t: ParserType, e: TypedExpression, name: String) extends TypedExpression(t)

/**
 * RecordConstruction
 */
case class AttributeConstruction(name: String, e: TypedExpression) extends Positional
case class RecordConstruction(t: ParserType, atts: List[AttributeConstruction]) extends TypedExpression(t)

/**
 * IfThenElse
 */
case class IfThenElse(t: ParserType, e1: TypedExpression, e2: TypedExpression, e3: TypedExpression) extends TypedExpression(t)

/**
 * BinaryOperation
 */
case class BinaryOperation(t: ParserType, op: BinaryOperator, e1: TypedExpression, e2: TypedExpression) extends TypedExpression( if (true ) t else t)

/**
 * FunctionAbstraction
 */
case class FunctionAbstraction(t: ParserType, v: Variable, e: TypedExpression) extends TypedExpression(t)

/**
 * FunctionApplication
 */
case class FunctionApplication(t: ParserType, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/**
 * Zeroes for Collection Monoids
 *  
 * The following are classes and not an objects so that they holds their positions.
 * (They inherit from TypedExpression -> Expression -> **Positional**).
 * 
 */
case class EmptySet() extends TypedExpression(SetType(VariableType()))
case class EmptyBag() extends TypedExpression(BagType(VariableType()))
case class EmptyList() extends TypedExpression(ListType(VariableType()))

/**
 * ConsCollectionMonoid
 */
case class ConsCollectionMonoid(t: ParserType, m: CollectionMonoid, e: TypedExpression) extends TypedExpression(t)

/**
 * MergeMonoid
 */
case class MergeMonoid(t: ParserType, m: Monoid, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/**
 * Comprehension
 */
case class Comprehension(t: ParserType, m: Monoid, e: TypedExpression, qs: List[Expression]) extends TypedExpression(t)

/**
 * Generator
 */
case class Generator(v: Variable, e: TypedExpression) extends UntypedExpression

/**
 * Unary Functions
 */
case class Not(e: TypedExpression) extends TypedExpression(BoolType)
case class Neg(e: TypedExpression) extends TypedExpression(e.parserType)
//case class FloatToInt(e: TypedExpression) extends TypedExpression(e.parserType match { case FloatType => IntType; case _ => throw RawInternalException("foo") })
sealed abstract class FloatExpression extends TypedExpression(FloatType)
sealed abstract class IntExpression extends TypedExpression(IntType)
case class FloatToInt(e: FloatExpression) extends IntExpression
case class FloatToString(e: TypedExpression) extends TypedExpression(StringType)
case class IntToFloat(e: TypedExpression) extends TypedExpression(FloatType)
case class IntToString(e: TypedExpression) extends TypedExpression(StringType)
case class StringToBool(e: TypedExpression) extends TypedExpression(BoolType)
case class StringToInt(e: TypedExpression) extends TypedExpression(IntType)
case class StringToFloat(e: TypedExpression) extends TypedExpression(FloatType)

/**
 * Bind
 */
case class Bind(v: Variable, e: TypedExpression) extends UntypedExpression

/**
 * CalculusPrettyPrinter
 */
object CalculusPrettyPrinter {
  def apply(e: Expression): String = e match {
    case Null()                                   => "null"
    case BoolConst(v)                             => if (v) "true" else "false"
    case IntConst(v)                              => v.toString
    case FloatConst(v)                            => v.toString
    case StringConst(v)                           => "\"" + v.toString + "\""
    case Variable(_, name)                        => name
    case RecordProjection(_, e, name)             => CalculusPrettyPrinter(e) + "." + name
    case RecordConstruction(_, atts)              => "( " + atts.map(att => att.name + " := " + CalculusPrettyPrinter(att.e)).mkString(", ") + " )"
    case IfThenElse(_, e1, e2, e3)                => "if " + CalculusPrettyPrinter(e1) + " then " + CalculusPrettyPrinter(e2) + " else " + CalculusPrettyPrinter(e3)
    case BinaryOperation(_, op, e1, e2)           => "( " + CalculusPrettyPrinter(e1) + " " + BinaryOperatorPrettyPrinter(op) + " " + CalculusPrettyPrinter(e2) + " )"
    case FunctionAbstraction(_, v, e)             => "\\" + CalculusPrettyPrinter(v) + " : " + CalculusPrettyPrinter(e)
    case FunctionApplication(_, e1, e2)           => CalculusPrettyPrinter(e1) + "(" + CalculusPrettyPrinter(e2) + ")"
    case EmptySet()                               => "{}"
    case EmptyBag()                               => "bag{}"
    case EmptyList()                              => "[]"
    case ConsCollectionMonoid(_, SetMonoid(), e)  => "{ " + CalculusPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, BagMonoid(), e)  => "bag{ " + CalculusPrettyPrinter(e) + " }"
    case ConsCollectionMonoid(_, ListMonoid(), e) => "[ " + CalculusPrettyPrinter(e) + " ]"
    case MergeMonoid(_, m, e1, e2)                => "( " + CalculusPrettyPrinter(e1) + " merge " + MonoidPrettyPrinter(m) + " " + CalculusPrettyPrinter(e2) + " )"
    case Comprehension(_, m, e, qs)               => "for ( " + qs.map(CalculusPrettyPrinter(_)).mkString(", ") + " ) yield " + MonoidPrettyPrinter(m) + " " + CalculusPrettyPrinter(e)
    case Generator(v, e)                          => CalculusPrettyPrinter(v) + " <- " + CalculusPrettyPrinter(e)
    case Not(e)                                   => "not(" + CalculusPrettyPrinter(e) + ")"
    case FloatToInt(e)                            => "as_int(" + CalculusPrettyPrinter(e) + ")"
    case FloatToString(e)                         => "as_string(" + CalculusPrettyPrinter(e) + ")"
    case IntToFloat(e)                            => "as_float(" + CalculusPrettyPrinter(e) + ")"
    case IntToString(e)                           => "as_string(" + CalculusPrettyPrinter(e) + ")"
    case StringToBool(e)                          => "as_bool(" + CalculusPrettyPrinter(e) + ")"
    case StringToInt(e)                           => "as_int(" + CalculusPrettyPrinter(e) + ")"
    case StringToFloat(e)                         => "as_float(" + CalculusPrettyPrinter(e) + ")"
    case Bind(v, e)                               => CalculusPrettyPrinter(v) + " := " + CalculusPrettyPrinter(e)
  }
}