package raw.calculus

import org.kiama.util.TreeNode

/** Path: only used for the canonical form
 */
sealed abstract class Path extends TreeNode

case class BoundVar(v: CanonicalCalculus.Var) extends Path {
  override def toString() = s"$v"
}

case class ClassExtent(name: String) extends Path {
  override def toString() = s"$name"
}

case class InnerPath(p: Path, name: String) extends Path {
  override def toString() = s"$p.$name"
}
