package raw
package calculus

/** Calculus
  */
object Calculus {

  import org.kiama.relation.Tree
  import scala.collection.immutable.Seq

  /** Tree type for Calculus.
    */
  type Calculus = Tree[RawNode,Exp]

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Base class for all nodes.
    */
  sealed abstract class CalculusNode extends RawNode

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

  case class StringConst(value: String) extends Const {
    type T = String
  }

  sealed abstract class NumberConst extends Const

  case class IntConst(value: Integer) extends NumberConst {
    type T = Integer
  }

  case class FloatConst(value: Float) extends NumberConst {
    type T = Float
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
  case class IdnExp(idn: IdnUse) extends Exp

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

  /** Comprehension in canonical form, i.e. with paths and predicates in CNF.
    * For details in the canonical form, refer to [1] page 19.
    */
  case class CanonicalComp(m: Monoid, paths: List[GenPath], preds: List[Exp], e: Exp) extends Exp

  /** Generator using paths.
    * This is only used by the canonical form.
    */
  case class GenPath(idn: IdnDef, p: Path) extends CalculusNode

  /** Paths.
    * These are only used by the canonical form.
    */
  sealed abstract class Path extends CalculusNode

  case class BoundPath(idn: IdnUse) extends Path

  case class InnerPath(p: Path, idn: Idn) extends Path

  case class ClassExtent(idn: Idn) extends Path

}
