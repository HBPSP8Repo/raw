package raw

import org.kiama.util.TreeNode

/** Binary Operator
  */
sealed abstract class BinaryOperator extends TreeNode

/** Equality Operator
  */
sealed abstract class EqualityOperator extends BinaryOperator

/** Equals
  */
case class Eq() extends EqualityOperator

/** Not equals
  */
case class Neq() extends EqualityOperator

/** Comparison Operator
  */
sealed abstract class ComparisonOperator extends EqualityOperator

/** Greater or equal than
  */
case class Ge() extends ComparisonOperator

/** Greater than
  */
case class Gt() extends ComparisonOperator

/** Less or equal than
  */
case class Le() extends ComparisonOperator

/** Less than
  */
case class Lt() extends ComparisonOperator

/** Arithmetic Operator
  */
sealed abstract class ArithmeticOperator extends BinaryOperator

/** Subtraction
  */
case class Sub() extends ArithmeticOperator

/** Division
  */
case class Div() extends ArithmeticOperator