package raw

/** Binary Operator
  */
sealed abstract class BinaryOperator extends RawNode

/** Greater or equal than
  */
case class Ge() extends BinaryOperator

/** Greater than
  */
case class Gt() extends BinaryOperator

/** Less or equal than
  */
case class Le() extends BinaryOperator

/** Less than
  */
case class Lt() extends BinaryOperator

/** Equals
  */
case class Eq() extends BinaryOperator

/** Not equals
  */
case class Neq() extends BinaryOperator

/** Plus
  */
case class Plus() extends BinaryOperator

/** Subtraction
  */
case class Sub() extends BinaryOperator

/** Multiplication
  */
case class Mult() extends BinaryOperator

/** Division
  */
case class Div() extends BinaryOperator

/** Modulo
 */
case class Mod() extends BinaryOperator

/** And
  */
case class And() extends BinaryOperator

/** Or
  */
case class Or() extends BinaryOperator

/** Max
  */
case class MaxOp() extends BinaryOperator

/** Min
  */
case class MinOp() extends BinaryOperator

/** Union
  */
case class Union() extends BinaryOperator

/** BagUnion
  */
case class BagUnion() extends BinaryOperator

/** Append
  */
case class Append() extends BinaryOperator

/** LIKE
  */
case class Like() extends BinaryOperator

/** NOT LIKE
  */
case class NotLike() extends BinaryOperator

/** IN
  */
case class In() extends BinaryOperator

/** NOT IN
  */
case class NotIn() extends BinaryOperator
