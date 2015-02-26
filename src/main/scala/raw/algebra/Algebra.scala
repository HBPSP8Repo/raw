package raw
package algebra

object Algebra {

  import org.kiama.relation.Tree
  import scala.collection.immutable.Seq

  /** Tree type for Calculus
    */
  type Algebra = Tree[RawNode,OperatorNode]

  /** Base class for all Algebra nodes.
    */
  sealed abstract class AlgebraNode extends RawNode

  /** Operator Nodes
    */
  sealed abstract class OperatorNode extends AlgebraNode

  /** Scan
    */
  case class Scan(name: String) extends OperatorNode

  /** Reduce
    */
  case class Reduce(m: Monoid, e: Exp, p: Exp, child: OperatorNode) extends OperatorNode

  /** Nest
    */
  case class Nest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: OperatorNode) extends OperatorNode

  /** Select
    */
  case class Select(p: Exp, child: OperatorNode) extends OperatorNode

  /** Join
    */
  case class Join(p: Exp, left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** Unnest
    */
  case class Unnest(path: Exp, pred: Exp, child: OperatorNode) extends OperatorNode

  /** OuterJoin
    */
  case class OuterJoin(p: Exp, left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** OuterUnnest
    */
  case class OuterUnnest(path: Exp, pred: Exp, child: OperatorNode) extends OperatorNode

  /** Merge
    */
  case class Merge(m: Monoid, left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Expressions
    */
  sealed abstract class Exp extends AlgebraNode

  /** Null
    */
  case object Null extends Exp

  /** Constants
    */
  sealed abstract class Const extends Exp

  // TODO: Add DateTime, smaller/larger integers/floats.
  case class BoolConst(value: Boolean) extends Const

  case class StringConst(value: String) extends Const

  sealed abstract class NumberConst extends Const

  case class IntConst(value: String) extends NumberConst

  case class FloatConst(value: String) extends NumberConst

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
  case class AttrCons(idn: Idn, e: Exp) extends AlgebraNode

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