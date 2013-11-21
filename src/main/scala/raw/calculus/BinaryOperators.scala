package raw.calculus

import scala.util.parsing.input.Positional

/** BinaryOperator
 */

sealed abstract class BinaryOperator extends Positional

/** BinaryOperator
 */

sealed abstract class ComparisonOperator extends BinaryOperator

case class Eq extends ComparisonOperator
case class Neq extends ComparisonOperator
case class Ge extends ComparisonOperator
case class Gt extends ComparisonOperator
case class Le extends ComparisonOperator
case class Lt extends ComparisonOperator

sealed abstract class ArithmeticOperator extends BinaryOperator

case class Add extends ArithmeticOperator
case class Sub extends ArithmeticOperator
case class Mult extends ArithmeticOperator
case class Div extends ArithmeticOperator
