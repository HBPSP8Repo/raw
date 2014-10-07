package raw.logical.calculus.parser

import scala.util.parsing.input.Positional

/**
 * BinaryOperator
 *  
 * The binary operators are redefined in the calculus parser as classes instead of objects since they inherit from Positional.
 */
sealed abstract class BinaryOperator extends Positional

/**
 * ComparisonOperator
 */
sealed abstract class ComparisonOperator extends BinaryOperator

case class Eq() extends ComparisonOperator
case class Neq() extends ComparisonOperator
case class Ge() extends ComparisonOperator
case class Gt() extends ComparisonOperator
case class Le() extends ComparisonOperator
case class Lt() extends ComparisonOperator

/**
 * ArithmeticOperator
 */
sealed abstract class ArithmeticOperator extends BinaryOperator

case class Add() extends ArithmeticOperator
case class Sub() extends ArithmeticOperator
case class Mult() extends ArithmeticOperator
case class Div() extends ArithmeticOperator

/**
 * BinaryOperatorPrettyPrinter
 */
object BinaryOperatorPrettyPrinter {
  def apply(op: BinaryOperator) = op match {
    case Eq() => "="
    case Neq() => "<>"
    case Ge() => ">="
    case Gt() => ">"
    case Le() => "<="
    case Lt() => "<"
    case Add() => "+"
    case Sub() => "-"
    case Mult() => "*"
    case Div() => "/"
  }
}
