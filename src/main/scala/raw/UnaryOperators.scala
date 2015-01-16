package raw

import org.kiama.util.TreeNode

/** Unary Operator
 */
sealed abstract class UnaryOperator extends TreeNode

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