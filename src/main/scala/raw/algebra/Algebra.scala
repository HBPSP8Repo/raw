package raw.algebra

import org.kiama.util.TreeNode
import raw.calculus.CanonicalCalculus.{Exp, Var}
import raw.calculus.{Monoid, Path}

/** Algebra
  */

// TODO: Algebra depends on Calculus (for Exp, Path, Var) while Calculus.Unnester depends on Algebra as well; doesn't sound right.

sealed abstract class AlgebraNode extends TreeNode

case class Scan(name: String) extends AlgebraNode {
  override def toString() = "Scan $name"
}

case class Reduce(m: Monoid, e: Exp, p: List[Exp], child: AlgebraNode) extends AlgebraNode

// TODO: Rename `p` to `ps` to have consistent names; or to `preds` even?
case class Nest(m: Monoid, e: Exp, f: List[Var], p: List[Exp], g: List[Var], child: AlgebraNode) extends AlgebraNode

case class Select(ps: List[Exp], child: AlgebraNode) extends AlgebraNode

case class Join(ps: List[Exp], left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

case class Unnest(path: Path, p: List[Exp], child: AlgebraNode) extends AlgebraNode

case class OuterJoin(p: List[Exp], left: AlgebraNode, right: AlgebraNode) extends AlgebraNode

case class OuterUnnest(path: Path, p: List[Exp], child: AlgebraNode) extends AlgebraNode

case class Merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode
