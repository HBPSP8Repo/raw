package raw
package algebra

object LogicalAlgebra {

  import org.kiama.relation.Tree
  import Expressions.Exp

  /** Tree type for Calculus
    */
  type Algebra = Tree[RawNode,LogicalAlgebraNode]

  /** Logical Algebra Nodes
    */
  sealed abstract class LogicalAlgebraNode extends AlgebraNode

  /** Scan
    */
  case class Scan(name: String) extends LogicalAlgebraNode

  /** Reduce
    */
  case class Reduce(m: Monoid, e: Exp, p: Exp, child: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** Nest
    */
  case class Nest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** Select
    */
  case class Select(p: Exp, child: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** Join
    */
  case class Join(p: Exp, left: LogicalAlgebraNode, right: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** Unnest
    */
  case class Unnest(path: Exp, pred: Exp, child: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** OuterJoin
    */
  case class OuterJoin(p: Exp, left: LogicalAlgebraNode, right: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** OuterUnnest
    */
  case class OuterUnnest(path: Exp, pred: Exp, child: LogicalAlgebraNode) extends LogicalAlgebraNode

  /** Merge
    */
  case class Merge(m: Monoid, left: LogicalAlgebraNode, right: LogicalAlgebraNode) extends LogicalAlgebraNode

}