package raw.calculus

import scala.util.parsing.input.Positional

/** Expressions for Calculus
 */

sealed abstract class Expression extends Positional

sealed abstract class TypedExpression(val monoidType: MonoidType) extends Expression

sealed abstract class UntypedExpression extends Expression

/** Null
 */

case class Null extends TypedExpression(VariableType())

/** Constant
 */

sealed abstract class Constant(t: MonoidType) extends TypedExpression(t)
case class BoolConst(v: Boolean) extends Constant(BoolType)
case class IntConst(v: Long) extends Constant(IntType)
case class FloatConst(v: Double) extends Constant(FloatType)
case class StringConst(v: String) extends Constant(StringType)

/** Variable
 */

case class Variable(t: MonoidType) extends TypedExpression(t) {
  override def equals(o: Any) = super.equals(o)
  override def hashCode = super.hashCode    
}

/** ClassExtent
 */
 
case class ClassExtent(t: MonoidType, id: String) extends TypedExpression(t)

/** RecordProjection
 */

case class RecordProjection(t: MonoidType, e: TypedExpression, name: String) extends TypedExpression(t)

/** RecordConstruction
 */

case class AttributeConstruction(name: String, e: TypedExpression) extends Positional
case class RecordConstruction(t: MonoidType, atts: List[AttributeConstruction]) extends TypedExpression(t)

/** IfThenElse
 */

case class IfThenElse(t: MonoidType, e1: TypedExpression, e2: TypedExpression, e3: TypedExpression) extends TypedExpression(t)

/** BinaryOperation
 */

case class BinaryOperation(t: MonoidType, op: BinaryOperator, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/** FunctionAbstraction
 */

case class FunctionAbstraction(t: MonoidType, v: Variable, e: TypedExpression) extends TypedExpression(t)

/** FunctionApplication
 */

case class FunctionApplication(t: MonoidType, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)


/** Zeroes for Collection Monoids
 */

case class EmptySet extends TypedExpression(VariableType())
case class EmptyBag extends TypedExpression(VariableType())
case class EmptyList extends TypedExpression(VariableType())

/** ConsCollectionMonoid
 */

case class ConsCollectionMonoid(t: MonoidType, m: CollectionMonoid, e: TypedExpression) extends TypedExpression(t)

/** MergeMonoid
 */

case class MergeMonoid(t: MonoidType, m: Monoid, e1: TypedExpression, e2: TypedExpression) extends TypedExpression(t)

/** Comprehension
 */

case class Comprehension(t: MonoidType, m: Monoid, e: TypedExpression, qs: List[Expression]) extends TypedExpression(t)

/** Generator
 */

case class Generator(v: Variable, e: TypedExpression) extends UntypedExpression

/** Unary Functions
 * 
 * TODO: Why aren't unary functions included in [1] (Fig. 2, page 12)?
 */

case class Not(e: TypedExpression) extends TypedExpression(BoolType)

/** Bind
 */

case class Bind(v: Variable, e: TypedExpression) extends UntypedExpression