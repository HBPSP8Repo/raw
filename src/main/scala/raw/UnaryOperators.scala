package raw

/** Unary Operator
 */
sealed abstract class UnaryOperator extends RawNode

/** Not
 */
case class Not() extends UnaryOperator

/** Negation
 */
case class Neg() extends UnaryOperator

/** Convert to boolean
 */
case class ToBool() extends UnaryOperator

/** Convert to number
 */
case class ToNumber() extends UnaryOperator

/** Convert to string
 */
case class ToString() extends UnaryOperator