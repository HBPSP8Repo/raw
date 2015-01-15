package raw.algebra

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