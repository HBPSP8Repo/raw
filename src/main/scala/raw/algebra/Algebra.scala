package raw
package algebra

object Algebra {

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
  case class Reduce(m: Monoid, e: Exp, p: Exp, child: AlgebraNode) extends AlgebraNode

  /** Nest
    */
  case class Nest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: AlgebraNode) extends AlgebraNode

  /** Select
    */
  case class Select(p: Exp, child: AlgebraNode) extends AlgebraNode

  /** Join
    */
  case class Join(p: Exp, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** Unnest
    */
  case class Unnest(path: Exp, pred: Exp, child: AlgebraNode) extends AlgebraNode

  /** OuterJoin
    */
  case class OuterJoin(p: Exp, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** OuterUnnest
    */
  case class OuterUnnest(path: Exp, pred: Exp, child: AlgebraNode) extends AlgebraNode

  /** Merge
    */
  case class Merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode
}