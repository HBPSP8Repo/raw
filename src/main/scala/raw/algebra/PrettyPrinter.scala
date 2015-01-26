package raw.algebra

import raw._

/** AlgebraPrettyPrinter
  */
abstract class AlgebraPrettyPrinter extends PrettyPrinter {
  def path(p: Path): Doc = p match {
    case BoundArg(a)        => exp(a)
    case ClassExtent(name)  => name
    case InnerPath(p, name) => path(p) <> dot <> name
  }

  def preds(ps: List[Exp]): Doc = list(ps, prefix = "predicates", elemToDoc = exp)

  def args(as: List[Arg]) = list(as, elemToDoc = exp)

  def exp(e: ExpNode): Doc = e match {
    case Null                        => "null"
    case c: Const                    => c.value.toString()
    case Arg(i)                      => s"$$$i"
    case RecordProj(e, idn)          => exp(e) <> dot <> idn
    case AttrCons(idn, e)            => idn <+> ":=" <+> exp(e)
    case RecordCons(atts)            => list(atts.toList, prefix = "", elemToDoc = exp)
    case IfThenElse(e1, e2, e3)      => "if" <+> exp(e1) <+> "then" <+> exp(e2) <+> "else" <+> exp(e3)
    case BinaryExp(op, e1, e2)       => exp(e1) <+> binaryOp(op) <+> exp(e2)
    case ZeroCollectionMonoid(m)     => collection(m, empty)
    case ConsCollectionMonoid(m, e)  => collection(m, exp(e))
    case MergeMonoid(m, e1, e2)      => exp(e1) <+> merge(m) <+> exp(e2)
    case UnaryExp(op, e)             => unaryOp(op) <+> exp(e)
  }
}

/** Logica;AlgebraPrettyPrinter
  */
object LogicalAlgebraPrettyPrinter extends AlgebraPrettyPrinter {

  import LogicalAlgebra._

  def pretty(a: AlgebraNode): String =
    super.pretty(show(a))

  def show(a: AlgebraNode): Doc = a match {
    case Scan(name)                  => "scan" <+> parens(text(name))
    case Reduce(m, e, ps, child)     => "reduce" <+> monoid(m) <+> exp(e) <+> preds(ps) <@> nest(show(child))
    case Nest(m, e, f, ps, g, child) => "nest" <+> monoid(m) <+> exp(e) <+> args(f) <+> preds(ps) <+> args(g) <@> nest(show(child))
    case Select(ps, child)           => "select" <+> preds(ps) <@> nest(show(child))
    case Join(ps, left, right)       => "join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(p, ps, child)        => "unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case OuterJoin(ps, left, right)  => "outer_join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(p, ps, child)   => "outer_unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
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
    case Reduce(m, e, ps, child)     => "reduce" <+> monoid(m) <+> exp(e) <+> preds(ps) <@> nest(show(child))
    case Nest(m, e, f, ps, g, child) => "nest" <+> monoid(m) <+> exp(e) <+> args(f) <+> preds(ps) <+> args(g) <@> nest(show(child))
    case Select(ps, child)           => "select" <+> preds(ps) <@> nest(show(child))
    case Join(ps, left, right)       => "join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case Unnest(p, ps, child)        => "unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case OuterJoin(ps, left, right)  => "outer_join" <+> preds(ps) <@> nest(show(left)) <@> nest(show(right))
    case OuterUnnest(p, ps, child)   => "outer_unnest" <+> path(p) <+> preds(ps) <@> nest(show(child))
    case Merge(m, left, right)       => "merge" <+> monoid(m) <@> nest(show(left)) <@> nest(show(right))
  }
}