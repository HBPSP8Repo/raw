package raw
package algebra

/** AlgebraPrettyPrinter
  */
object LogicalAlgebraPrettyPrinter extends PrettyPrinter {

  import LogicalAlgebra._

  def apply(n: LogicalAlgebraNode): String =
    super.pretty(show(n)).layout

  def show(a: LogicalAlgebraNode): Doc = a match {
    case Scan(name, _)                  => "scan" <+> parens(name)
    case Reduce(m, e, p, child)         => "reduce" <+> monoid(m) <+> ExpressionsPrettyPrinter(e) <+> ExpressionsPrettyPrinter(p) <@> nest(show(child))
    case Nest(m, e, f, p, g, child)     => "nest" <+> monoid(m) <+> ExpressionsPrettyPrinter(e) <+> ExpressionsPrettyPrinter(f) <+> ExpressionsPrettyPrinter(p) <+> ExpressionsPrettyPrinter(g) <@> nest(show(child))
    case Select(p, child)               => "select" <+> ExpressionsPrettyPrinter(p) <@> nest(show(child))
    case Join(p, left, right)           => "join" <+> ExpressionsPrettyPrinter(p) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(path, pred, child)      => "unnest" <+> ExpressionsPrettyPrinter(path) <+> ExpressionsPrettyPrinter(pred) <@> nest(show(child))
    case OuterJoin(p, left, right)      => "outer_join" <+> ExpressionsPrettyPrinter(p) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(path, pred, child) => "outer_unnest" <+> ExpressionsPrettyPrinter(path) <+> ExpressionsPrettyPrinter(pred) <@> nest(show(child))
    case Merge(m, left, right)          => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
  }
}