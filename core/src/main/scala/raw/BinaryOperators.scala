package raw

/** Binary Operator
  */
sealed abstract class BinaryOperator extends RawNode

/** Comparison Operator
  */
sealed abstract class ComparisonOperator extends BinaryOperator

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

/** Equality Operator
  */
sealed abstract class EqualityOperator extends ComparisonOperator

/** Equals
  */
case class Eq() extends EqualityOperator

/** Not equals
  */
case class Neq() extends EqualityOperator

/** Arithmetic Operator
  */
sealed abstract class ArithmeticOperator extends BinaryOperator

/** Subtraction
  */
case class Sub() extends ArithmeticOperator

/** Division
  */
case class Div() extends ArithmeticOperator

/** Modulo
 */
case class Mod() extends ArithmeticOperator