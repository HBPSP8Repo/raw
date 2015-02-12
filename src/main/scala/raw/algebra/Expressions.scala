package raw
package algebra

object Expressions {

  sealed abstract class ExpNode extends Product

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Expressions
    */
  sealed abstract class Exp extends ExpNode

  /** Null
    */
  case object Null extends Exp

  /** Constants
    */
  sealed abstract class Const extends Exp {
    type T

    def value: T
  }

  // TODO: Add DateTime, smaller/larger integers/floats.
  case class BoolConst(value: Boolean) extends Const {
    type T = Boolean
  }

  case class IntConst(value: Integer) extends Const {
    type T = Integer
  }

  case class FloatConst(value: Float) extends Const {
    type T = Float
  }

  case class StringConst(value: String) extends Const {
    type T = String
  }

  /** Argument
   */
  case class Arg(idx: Int) extends Exp

  /** Product Projection
    */
  case class ProductProj(e: Exp, idx: Int) extends Exp

  /** Product Construction
    */
  case class ProductCons(es: Seq[Exp]) extends Exp

  /** Record Projection
    */
  case class RecordProj(e: Exp, idn: Idn) extends Exp

  /** Record Construction
    */
  case class AttrCons(idn: Idn, e: Exp) extends ExpNode

  case class RecordCons(atts: Seq[AttrCons]) extends Exp

  /** If/Then/Else
    */
  case class IfThenElse(e1: Exp, e2: Exp, e3: Exp) extends Exp

  /** Binary Expression
    */
  case class BinaryExp(op: BinaryOperator, e1: Exp, e2: Exp) extends Exp

  /** Zero for Collection Monoid
    */
  case class ZeroCollectionMonoid(m: CollectionMonoid) extends Exp

  /** Construction for Collection Monoid
    */
  case class ConsCollectionMonoid(m: CollectionMonoid, e: Exp) extends Exp

  /** Merge Monoid
    */
  case class MergeMonoid(m: Monoid, e1: Exp, e2: Exp) extends Exp

  /** Unary Expression
    */
  case class UnaryExp(op: UnaryOperator, e: Exp) extends Exp

}