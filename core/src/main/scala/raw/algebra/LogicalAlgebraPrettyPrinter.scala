package raw
package algebra

/** AlgebraPrettyPrinter
  */
object LogicalAlgebraPrettyPrinter extends PrettyPrinter {

  import LogicalAlgebra._

  def apply(n: LogicalAlgebraNode): String =
    super.pretty(show(n)).layout

  def show(a: LogicalAlgebraNode): Doc = a match {
    case Scan(name, _)                  => "scan" <> parens(s""""$name"""")
    case Reduce(m, e, p, child)         => "reduce" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(p), nest(show(child))), comma))))
    case Nest(m, e, f, p, g, child)     => "nest" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(f), ExpressionsPrettyPrinter(p), ExpressionsPrettyPrinter(g), nest(show(child))), comma))))
    case Select(p, child)               => "select" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(child))), comma))))
    case Join(p, left, right)           => "join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))
    case Unnest(path, pred, child)      => "unnest" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))
    case OuterJoin(p, left, right)      => "outer_join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))
    case OuterUnnest(path, pred, child) => "outer_unnest" <> parens(group(nest(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))
    case Merge(m, left, right)          => "merge" <> parens(nest(group(lsep(List(monoid(m), nest(show(left)), nest(show(right))), comma))))
    case Assign(as, child)              =>
      /* Kiama pretty printer expects sequences to be instances of s.c.immutable.Seq, while the Assign node has a
       * s.c.Seq instance. They are not compatible, so we have to convert it here to List, which is implements the
       * immutable Seq.
       */
      val assignDocs = as.map({case (key, node) => text(s"$key := ") <> show(node)}).to[List]
      "assign" <> parens(nest(group(lsep( assignDocs ++ List(nest(show(child))), comma))))
  }
}