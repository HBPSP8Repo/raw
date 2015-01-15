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

/** AlgebraPrettyPrinter
  */

object AlgebraPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def pretty(a: AlgebraNode): String =
    super.pretty(show(a))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                  => "scan" <+> parens(text(name))
    case Reduce(m, e, p, child)      => "reduce" <+> text(m.toString()) <+> text(e.toString()) <+> list(p) <@> nest(show(child))
    case Nest(m, e, f, p, g, child)  => "nest" <+> text(m.toString()) <+> text(e.toString()) <+> list(f) <+> list(p) <+> list(g) <@> nest(show(child))
    case Select(ps, child)           => "select" <+> list(ps) <@> nest(show(child))
    case Join(ps, left, right)       => "join" <+> list(ps) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(path, p, child)      => "unnest" <+> text(path.toString()) <+> list(p) <@> nest(show(child))
    case OuterJoin(ps, left, right)  => "outer_join" <+> list(ps) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(path, p, child) => "outer_unnest" <+> text(path.toString()) <+> list(p) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> text(m.toString()) <@> nest(show(left)) <@> nest(show(right))

    //    case Scan(name) => "Scan " + name
    //    case Reduce(m, e, p, x) => "Reduce " + MonoidPrettyPrinter(m) + " [ e = " + ExpressionPrettyPrinter(e) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    //    case Nest(m, e, f, p, g, x) => "Nest " + MonoidPrettyPrinter(m) + " [ e = " + ExpressionPrettyPrinter(e) + " ] [ f = " +  ListArgumentPrettyPrinter(f) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] [ g = " + ListArgumentPrettyPrinter(g) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    //    case Select(p, x) => "Select [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    //    case Join(p, x, y) => "Join [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    //    case Unnest(path, p, x) => "Unnest [ path = " + PathPrettyPrinter(path) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    //    case OuterJoin(p, x, y) => "OuterJoin [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    //    case OuterUnnest(path, p, x) => "OuterUnnest [ path = " + PathPrettyPrinter(path) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    //    case Merge(m,x,y) => "Merge " + MonoidPrettyPrinter(m) + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    //    case Empty => "Empty"
  }

}