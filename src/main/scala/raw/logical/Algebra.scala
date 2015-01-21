package raw.logical

import raw._

object Algebra {

  import org.kiama.util.TreeNode

  /** Identifiers are represented as strings
    */
  type Idn = String

  sealed abstract class AlgebraNode extends TreeNode

  /** Operator Nodes
    */

  sealed abstract class OperatorNode extends AlgebraNode

  /** Scan
    */
  case class Scan(name: Idn) extends OperatorNode

  /** Reduce
    */
  case class Reduce(m: Monoid, e: Exp, ps: List[Exp], child: OperatorNode) extends OperatorNode

  /** Nest
    */
  case class Nest(m: Monoid, e: Exp, f: List[Arg], ps: List[Exp], g: List[Arg], child: OperatorNode) extends OperatorNode

  /** Select
    */
  case class Select(ps: List[Exp], child: OperatorNode) extends OperatorNode

  /** Join
    */
  case class Join(ps: List[Exp], left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** Unnest
    */
  case class Unnest(p: Path, ps: List[Exp], child: OperatorNode) extends OperatorNode

  /** OuterJoin
    */
  case class OuterJoin(ps: List[Exp], left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** OuterUnnest
    */
  case class OuterUnnest(p: Path, ps: List[Exp], child: OperatorNode) extends OperatorNode

  /** Merge
    */
  case class Merge(m: Monoid, left: OperatorNode, right: OperatorNode) extends OperatorNode

  /** Expressions
    */
  sealed abstract class Exp extends AlgebraNode

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
  case class Arg(i: Int) extends Exp

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

  /** Path
    */
  sealed abstract class Path extends AlgebraNode

  case class BoundArg(a: Arg) extends Path

  case class ClassExtent(name: Idn) extends Path

  case class InnerPath(p: Path, name: Idn) extends Path

}