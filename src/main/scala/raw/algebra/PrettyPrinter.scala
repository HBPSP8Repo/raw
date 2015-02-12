package raw
package algebra

/** AlgebraPrettyPrinter
  */
object AlgebraPrettyPrinter extends PrettyPrinter {

  import Algebra._
  import Expressions._

  def pretty(n: AlgebraNode): String =
    super.pretty(show(n))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                  => "scan" <+> parens(text(name))
    case Reduce(m, e, p, child)     => "reduce" <+> monoid(m) <+> ExpressionPrettyPrinter.pretty(e) <+> ExpressionPrettyPrinter.pretty(p) <@> nest(show(child))
    case Nest(m, e, f, p, g, child) => "nest" <+> monoid(m) <+> ExpressionPrettyPrinter.pretty(e) <+> ExpressionPrettyPrinter.pretty(f) <+> ExpressionPrettyPrinter.pretty(p) <+> ExpressionPrettyPrinter.pretty(g) <@> nest(show(child))
    case Select(p, child)           => "select" <+> ExpressionPrettyPrinter.pretty(p) <@> nest(show(child))
    case Join(p, left, right)       => "join" <+> ExpressionPrettyPrinter.pretty(p) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(path, pred, child)        => "unnest" <+> ExpressionPrettyPrinter.pretty(path) <+> ExpressionPrettyPrinter.pretty(pred) <@> nest(show(child))
    case OuterJoin(p, left, right)  => "outer_join" <+> ExpressionPrettyPrinter.pretty(p) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(path, pred, child)   => "outer_unnest" <+> ExpressionPrettyPrinter.pretty(path) <+> ExpressionPrettyPrinter.pretty(pred) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
  }
}