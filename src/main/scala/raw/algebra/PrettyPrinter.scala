package raw
package algebra

/** AlgebraPrettyPrinter
  */
abstract class AlgebraPrettyPrinter extends PrettyPrinter {
  import Expressions._

  def path(p: Path): Doc = p match {
    case BoundArg(a)        => a
    case ClassExtent(name)  => name
    case InnerPath(p, name) => path(p) <> dot <> name
  }

  // TODO: Cleanup later
  def preds(ps: List[Exp]): Doc = "predicates " + ps.map(ExpressionsPrettyPrinter.show).mkString(", ")

  // TODO: Cleanup later
  def idnExps(es: List[IdnExp]) =  es.map(ExpressionsPrettyPrinter.show).mkString(", ")
}

/** Logica;AlgebraPrettyPrinter
  */
object LogicalAlgebraPrettyPrinter extends AlgebraPrettyPrinter {

  import LogicalAlgebra._

  def pretty(a: AlgebraNode): String =
    super.pretty(show(a))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                  => "scan" <+> parens(text(name))
    case Reduce(m, e, p, child)     => "reduce" <+> monoid(m) <+> ExpressionsPrettyPrinter.pretty(e) <+> ExpressionsPrettyPrinter.pretty(p) <@> nest(show(child))
    case Nest(m, e, f, p, g, child) => "nest" <+> monoid(m) <+> ExpressionsPrettyPrinter.pretty(e) <+> ExpressionsPrettyPrinter.pretty(f) <+> ExpressionsPrettyPrinter.pretty(p) <+> ExpressionsPrettyPrinter.pretty(g) <@> nest(show(child))
    case Select(p, child)           => "select" <+> ExpressionsPrettyPrinter.pretty(p) <@> nest(show(child))
    case Join(p, left, right)       => "join" <+> ExpressionsPrettyPrinter.pretty(p) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(path, pred, child)        => "unnest" <+> ExpressionsPrettyPrinter.pretty(path) <+> ExpressionsPrettyPrinter.pretty(pred) <@> nest(show(child))
    case OuterJoin(p, left, right)  => "outer_join" <+> ExpressionsPrettyPrinter.pretty(p) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(path, pred, child)   => "outer_unnest" <+> ExpressionsPrettyPrinter.pretty(path) <+> ExpressionsPrettyPrinter.pretty(pred) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
  }
}

object PhysicalAlgebraPrettyPrinter extends AlgebraPrettyPrinter {

  import PhysicalAlgebra._

  def pretty(a: AlgebraNode): String =
    super.pretty(show(a))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(tipe, location)                  => "scan" <+> (location match {
      case MemoryLocation(data) => parens("memory-data")
      case LocalFileLocation(path, fileType) => parens(path)
      case _ => parens("")
    })
    case Reduce(m, e, ps, child)     => "reduce" <+> monoid(m) <+> ExpressionsPrettyPrinter.pretty(e) <+> preds(ps) <@> nest(show(child))
    case Nest(m, e, f, ps, g, child) => "nest" <+> monoid(m) <+> ExpressionsPrettyPrinter.pretty(e) <+> idnExps(f) <+> preds(ps) <+> idnExps(g) <@> nest(show(child))
    case Select(ps, child)           => "select" <+> preds(ps) <@> nest(show(child))
    case Join(ps, left, right)       => "join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(p, ps, child)        => "unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case OuterJoin(ps, left, right)  => "outer_join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(p, ps, child)   => "outer_unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
  }
}