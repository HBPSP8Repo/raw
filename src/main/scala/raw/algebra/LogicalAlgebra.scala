package raw
package algebra

object LogicalAlgebra {

  import org.kiama.relation.Tree
  import Expressions._

  /** Tree type for Calculus
    */
  type Algebra = Tree[RawNode,AlgebraNode]

  /** Operator Nodes
    */

  sealed abstract class AlgebraNode extends RawNode

  /** Scan
    */
  case class Scan(name: String) extends AlgebraNode

  /** Reduce
    */
  case class Reduce(m: Monoid, e: FunAbs, p: FunAbs, child: AlgebraNode) extends AlgebraNode

  /** Nest
    */
  case class Nest(m: Monoid, e: FunAbs, f: FunAbs, p: FunAbs, g: FunAbs, child: AlgebraNode) extends AlgebraNode

  /** Select
    */
  case class Select(p: FunAbs, child: AlgebraNode) extends AlgebraNode

  /** Join
    */
  case class Join(p: FunAbs, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** Unnest
    */
  case class Unnest(path: FunAbs, pred: FunAbs, child: AlgebraNode) extends AlgebraNode

  /** OuterJoin
    */
  case class OuterJoin(p: FunAbs, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** OuterUnnest
    */
  case class OuterUnnest(path: FunAbs, pred: FunAbs, child: AlgebraNode) extends AlgebraNode

  /** Merge
    */
  case class Merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode
}