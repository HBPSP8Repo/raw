package raw.calculus

import org.kiama.util.TreeNode

/** Unary Operator
 */
sealed abstract class UnaryOperator extends TreeNode

/** Not
 */
case class Not() extends UnaryOperator {
  override def toString() = "not"
}

/** Negation
 */
case class Neg() extends UnaryOperator {
  override def toString() = "-"
}

/** Convert to boolean
 */
case class ToBool() extends UnaryOperator {
  override def toString() = "to_bool"
}

/** Convert to integer
 */
case class ToInt() extends UnaryOperator {
  override def toString() = "to_int"
}

/** Convert to float
 */
case class ToFloat() extends UnaryOperator {
  override def toString() = "to_float"
}

/** Convert to string
 */
case class ToString() extends UnaryOperator {
  override def toString() = "to_string"
}
