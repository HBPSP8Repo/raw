package raw

/** Unary Operator
 */
sealed abstract class UnaryOperator extends RawNode {
  override def toString() = PrettyPrinter.pretty(PrettyPrinter.unaryOp(this))
}

/** Not
 */
case class Not() extends UnaryOperator

/** Negation
 */
case class Neg() extends UnaryOperator

/** Convert to boolean
 */
case class ToBool() extends UnaryOperator

/** Convert to integer
 */
case class ToInt() extends UnaryOperator

/** Convert to float
 */
case class ToFloat() extends UnaryOperator

/** Convert to string
 */
case class ToString() extends UnaryOperator