package raw
package calculus

/** Calculus
  */
object Calculus {

  import org.kiama.relation.Tree

  import scala.collection.immutable.Seq

  /** Tree type for Calculus
    */
  type Calculus = Tree[RawNode,Exp]

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Base class for all Calculus nodes.
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
  sealed abstract class Const extends Exp

  case class BoolConst(value: Boolean) extends Const

  case class StringConst(value: String) extends Const

  sealed abstract class NumberConst extends Const

  case class IntConst(value: String) extends NumberConst

  case class FloatConst(value: String) extends NumberConst

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

  /** In Expression
    */
  case class InExp(e1: Exp, e2: Exp) extends Exp

  /** Function Abstraction
    */
  case class FunAbs(p: Pattern, e: Exp) extends Exp with Decl

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

  /** Declarations
    */
  sealed trait Decl extends Qual

  /** Generator
    */
  case class Gen(p: Pattern, e: Exp) extends Decl

  /** Bind
    */
  case class Bind(p: Pattern, e: Exp) extends Decl

  /** Sugar Nodes: removed by the Desugarer
    */
  sealed abstract class Sugar extends Exp

  /** Expression in a Block with Binds
    */
  case class ExpBlock(bs: Seq[Bind], e: Exp) extends Sugar

  /** Sum
    */
  case class Sum(e: Exp) extends Sugar

  /** Max
    */
  case class Max(e: Exp) extends Sugar

  /** Min
    */
  case class Min(e: Exp) extends Sugar

  /** Avg
    */
  case class Avg(e: Exp) extends Sugar

  /** Count
    */
  case class Count(e: Exp) extends Sugar

  /** Patterns
    */
  sealed abstract class Pattern extends CalculusNode
  case class PatternIdn(idn: IdnDef) extends Pattern
  case class PatternProd(ps: Seq[Pattern]) extends Pattern

  /** Canonical Comprehension
    */
  case class CanComp(m: Monoid, gs: Seq[Gen], ps: Seq[Exp], e: Exp) extends Exp

  /** Algebra Nodes
    */
  sealed abstract class AlgebraNode extends Exp

  /** Logical Algebra Nodes
    */
  sealed abstract class LogicalAlgebraNode extends AlgebraNode

  /** Reduce
    */
  case class Reduce(m: Monoid, child: Gen, e: Exp) extends LogicalAlgebraNode

  /** Nest
    */
  case class Nest(m: Monoid, child: Gen, k: Exp, p: Exp, e: Exp) extends LogicalAlgebraNode

  /** Filter
    */
  case class Filter(child: Gen, p: Exp) extends LogicalAlgebraNode

  /** Join
    */
  case class Join(left: Gen, right: Gen, p: Exp) extends LogicalAlgebraNode

  /** OuterJoin
    */
  case class OuterJoin(left: Gen, right: Gen, p: Exp) extends LogicalAlgebraNode

  /** Unnest
    */
  case class Unnest(child: Gen, path: Gen, pred: Exp) extends LogicalAlgebraNode

  /** OuterUnnest
    */
  case class OuterUnnest(child: Gen, path: Gen, pred: Exp) extends LogicalAlgebraNode

  /** Physical Algebra Nodes
    */
  sealed abstract class PhysicalAlgebraNode extends AlgebraNode

  /** HashJoin
    */
  case class HashJoin(left: Gen, right: Gen, es: Seq[Idn]) extends PhysicalAlgebraNode  // TODO: Fix signature

  /** Select
    */
  case class Select(from: Seq[Iterator],
                    distinct: Boolean,
                    group: Option[Exp],
                    proj: Exp,
                    where: Option[Exp],
                    order: Option[Exp],
                    having: Option[Exp]) extends Exp

  /** Iterator
    */
  case class Iterator(idn: Option[PatternIdn], e: Exp) extends Decl

  /** Partition
    */
  case class Partition() extends Exp

}
