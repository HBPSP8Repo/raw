package raw.calculus

import raw._

/** Calculus
  */
object Calculus {

  import org.kiama.util.TreeNode

  /** Identifiers are represented as strings
    */
  type Idn = String

  sealed abstract class CalculusNode extends TreeNode

  /** Qualifier
    */
  sealed abstract class Qual extends CalculusNode

  /** Expressions
    */
  sealed abstract class Exp extends Qual

  /** Null
    */
  case class Null() extends Exp

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

  /** Identifier reference
    */
  sealed abstract class IdnNode extends CalculusNode {
    def idn: String
  }

  /** Defining occurrence of an identifier
    */
  case class IdnDef(idn: Idn) extends IdnNode

  /** Use of an identifier
    */
  case class IdnUse(idn: Idn) extends IdnNode

  /** Identifier expression
    */
  case class IdnExp(name: IdnUse) extends Exp

  /** Record Projection
    */
  case class RecordProj(e: Exp, idn: Idn) extends Exp

  /** Record Construction
    */
  case class AttrCons(idn: Idn, e: Exp) extends CalculusNode

  case class RecordCons(atts: Seq[AttrCons]) extends Exp

  /** If/Then/Else
    */
  case class IfThenElse(e1: Exp, e2: Exp, e3: Exp) extends Exp

  /** Binary Expression
    */
  case class BinaryExp(op: BinaryOperator, e1: Exp, e2: Exp) extends Exp

  /** Function Application
    */
  case class FunApp(f: Exp, e: Exp) extends Exp

  /** Zero for Collection Monoid
    */
  case class ZeroCollectionMonoid(m: CollectionMonoid) extends Exp

  /** Construction for Collection Monoid
    */
  case class ConsCollectionMonoid(m: CollectionMonoid, e: Exp) extends Exp

  /** Merge Monoid
    */
  case class MergeMonoid(m: Monoid, e1: Exp, e2: Exp) extends Exp

  /** Comprehension
    */
  case class Comp(m: Monoid, qs: Seq[Qual], e: Exp) extends Exp

  /** Unary Expression
    */
  case class UnaryExp(op: UnaryOperator, e: Exp) extends Exp

  /** Function Abstraction
    */
  case class FunAbs(idn: IdnDef, t: Type, e: Exp) extends Exp

  /** Statements
    */
  sealed abstract class Statement extends Qual

  /** Generator
    */
  case class Gen(idn: IdnDef, e: Exp) extends Statement

  /** Bind
    */
  case class Bind(idn: IdnDef, e: Exp) extends Statement

}
