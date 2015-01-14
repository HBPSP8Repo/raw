package raw.calculus

import org.kiama.util.TreeNode

/** Binary Operator
 */
sealed abstract class BinaryOperator extends TreeNode

/** Equality Operator
 */
sealed abstract class EqualityOperator extends BinaryOperator

/** Equals
 */
case class Eq() extends EqualityOperator {
  override def toString() = "="
}

/** Not equals
 */
case class Neq() extends EqualityOperator {
  override def toString() = "<>"
}

/** Comparison Operator
 */
sealed abstract class ComparisonOperator extends EqualityOperator

/** Greater or equal than
 */
case class Ge() extends ComparisonOperator {
  override def toString() = ">="
}

/** Greater than
 */
case class Gt() extends ComparisonOperator {
  override def toString() = ">"
}

/** Less or equal than
 */
case class Le() extends ComparisonOperator {
  override def toString() = "<="
}

/** Less than
 */
case class Lt() extends ComparisonOperator {
  override def toString() = "<"
}

/** Arithmetic Operator
 */
sealed abstract class ArithmeticOperator extends BinaryOperator

/** Subtraction
 */
case class Sub() extends ArithmeticOperator {
  override def toString() = "-"
}

/** Division
 */
case class Div() extends ArithmeticOperator {
  override def toString() = "/"
}
