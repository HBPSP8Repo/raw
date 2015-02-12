package raw
package algebra

/** AlgebraPrettyPrinter
  */
object AlgebraPrettyPrinter extends PrettyPrinter {

  import Algebra._

  def pretty(n: AlgebraNode): String =
    super.pretty(show(n))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                     => "scan" <+> parens(text(name))
    case Reduce(m, e, p, child)         => "reduce" <+> monoid(m) <+> show(e) <+> show(p) <@> nest(show(child))
    case Nest(m, e, f, p, g, child)     => "nest" <+> monoid(m) <+> show(e) <+> show(f) <+> show(p) <+> show(g) <@> nest(show(child))
    case Select(p, child)               => "select" <+> show(p) <@> nest(show(child))
    case Join(p, left, right)           => "join" <+> show(p) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(path, pred, child)      => "unnest" <+> show(path) <+> show(pred) <@> nest(show(child))
    case OuterJoin(p, left, right)      => "outer_join" <+> show(p) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(path, pred, child) => "outer_unnest" <+> show(path) <+> show(pred) <@> nest(show(child))
    case Merge(m, left, right)          => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
    case Null                           => "null"
    case StringConst(v)                 => s""""$v""""
    case c: Const                       => c.value.toString()
    case Arg(idx)                       => s"$$$idx"
    case RecordProj(e, idn)             => show(e) <> dot <> idn
    case AttrCons(idn, e)               => idn <+> ":=" <+> show(e)
    case RecordCons(atts)               => list(atts.toList, prefix = "", elemToDoc = show)
    case IfThenElse(e1, e2, e3)         => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)          => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)        => collection(m, empty)
    case ConsCollectionMonoid(m, e)     => collection(m, show(e))
    case MergeMonoid(m, e1, e2)         => show(e1) <+> merge(m) <+> show(e2)
    case UnaryExp(op, e)                => unaryOp(op) <+> show(e)
    case ProductCons(es)                => "(" <+> es.map(show).mkString(",") <+> ")"
  }
}