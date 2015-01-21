package raw.logical

import raw._

/** AlgebraPrettyPrinter
  */
object AlgebraPrettyPrinter extends PrettyPrinter {

  import Algebra._

  def pretty(a: AlgebraNode): String =
    super.pretty(show(a))

  def path(p: Path): Doc = p match {
    case BoundArg(a)        => show(a)
    case ClassExtent(name)  => name
    case InnerPath(p, name) => path(p) <> dot <> name
  }

  def preds(ps: List[Exp]): Doc = list(ps, prefix = "predicates", elemToDoc = show)

  def args(as: List[Arg]) = list(as, elemToDoc = show)

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                  => "scan" <+> parens(text(name))
    case Reduce(m, e, ps, child)     => "reduce" <+> monoid(m) <+> show(e) <+> preds(ps) <@> nest(show(child))
    case Nest(m, e, f, ps, g, child) => "nest" <+> monoid(m) <+> show(e) <+> args(f) <+> preds(ps) <+> args(g) <@> nest(show(child))
    case Select(ps, child)           => "select" <+> preds(ps) <@> nest(show(child))
    case Join(ps, left, right)       => "join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(p, ps, child)        => "unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case OuterJoin(ps, left, right)  => "outer_join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(p, ps, child)   => "outer_unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
    case Null                        => "null"
    case c: Const                    => c.value.toString()
    case Arg(i)                      => s"$$$i"
    case RecordProj(e, idn)          => show(e) <> dot <> idn
    case AttrCons(idn, e)            => idn <+> ":=" <+> show(e)
    case RecordCons(atts)            => list(atts.toList, prefix = "", elemToDoc = show)
    case IfThenElse(e1, e2, e3)      => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)       => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)     => collection(m, empty)
    case ConsCollectionMonoid(m, e)  => collection(m, show(e))
    case MergeMonoid(m, e1, e2)      => show(e1) <+> merge(m) <+> show(e2)
    case UnaryExp(op, e)             => unaryOp(op) <+> show(e)
  }
}