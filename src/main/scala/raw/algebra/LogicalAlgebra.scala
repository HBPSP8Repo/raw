package raw.algebra

import raw._

object LogicalAlgebra {

  import org.kiama.util.TreeNode

  /** Operator Nodes
    */

  sealed abstract class AlgebraNode extends TreeNode

  /** Scan
    */
  case class Scan(name: String) extends AlgebraNode

  /** Reduce
    */
  case class Reduce(m: Monoid, e: Exp, ps: List[Exp], child: AlgebraNode) extends AlgebraNode

  /** Nest
    */
  case class Nest(m: Monoid, e: Exp, f: List[Arg], ps: List[Exp], g: List[Arg], child: AlgebraNode) extends AlgebraNode

  /** Select
    */
  case class Select(ps: List[Exp], child: AlgebraNode) extends AlgebraNode

  /** Join
    */
  case class Join(ps: List[Exp], left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** Unnest
    */
  case class Unnest(p: Path, ps: List[Exp], child: AlgebraNode) extends AlgebraNode

  /** OuterJoin
    */
  case class OuterJoin(ps: List[Exp], left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

  /** OuterUnnest
    */
  case class OuterUnnest(p: Path, ps: List[Exp], child: AlgebraNode) extends AlgebraNode

  /** Merge
    */
  case class Merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode
}