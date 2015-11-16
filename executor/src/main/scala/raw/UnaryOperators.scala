package raw

/** The unary operators.
  * Do NOT include sugar nodes, since these are the basic set of operations needed in code generation.
  * (Sugared operators should have their own Calculus node inheriting from Sugar.)
  */

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

/** Convert to integer
  */
case class ToInt() extends UnaryOperator

/** Convert to float
  */
case class ToFloat() extends UnaryOperator

/** Convert to string
  */
case class ToString() extends UnaryOperator

/** Convert to bag
  */
case class ToBag() extends UnaryOperator

/** Convert to list
  */
case class ToList() extends UnaryOperator

/** Convert to set
  */
case class ToSet() extends UnaryOperator
