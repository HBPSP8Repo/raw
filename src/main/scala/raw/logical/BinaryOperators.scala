package raw.logical

/** BinaryOperator
 */

sealed abstract class BinaryOperator

/** ComparisonOperator
 */

sealed abstract class ComparisonOperator extends BinaryOperator

case object Eq extends ComparisonOperator
case object Neq extends ComparisonOperator
case object Ge extends ComparisonOperator
case object Gt extends ComparisonOperator
case object Le extends ComparisonOperator
case object Lt extends ComparisonOperator

/** ArithmeticOperator
 */

sealed abstract class ArithmeticOperator extends BinaryOperator

case object Add extends ArithmeticOperator
case object Sub extends ArithmeticOperator
case object Mult extends ArithmeticOperator
case object Div extends ArithmeticOperator

/** BinaryOperatorPrettyPrinter
 */
object BinaryOperatorPrettyPrinter {
  def apply(op: BinaryOperator) = op match {
    case Eq => "="
    case Neq => "<>"
    case Ge => ">="
    case Gt => ">"
    case Le => "<="
    case Lt => "<"
    case Add => "+"
    case Sub => "-"
    case Mult => "*"
    case Div => "/"
  }
}
