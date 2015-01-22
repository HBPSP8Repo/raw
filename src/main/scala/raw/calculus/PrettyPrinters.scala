package raw.calculus

import raw._

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import Calculus._

  def pretty(n: CalculusNode, w: Width): String =
    super.pretty(show(n), w=w)

  def show(n: CalculusNode): Doc = n match {
    case _: Null                    => "null"
    case c: Const                   => c.value.toString()
    case IdnDef(idn)                => idn
    case IdnUse(idn)                => idn
    case IdnExp(name)               => show(name)
    case RecordProj(e, idn)         => show(e) <> dot <> idn
    case AttrCons(idn, e)           => idn <+> ":=" <+> show(e)
    case RecordCons(atts)           => list(atts.toList, prefix = "", elemToDoc = show)
    case IfThenElse(e1, e2, e3)     => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)      => show(e1) <+> binaryOp(op) <+> show(e2)
    case FunApp(f, e)               => show(f) <> parens(show(e))
    case ZeroCollectionMonoid(m)    => collection(m, empty)
    case ConsCollectionMonoid(m, e) => collection(m, show(e))
    case MergeMonoid(m, e1, e2)     => show(e1) <+> merge(m) <+> show(e2)
    case Comp(m, qs, e)             => "for" <+> list(qs.toList, prefix = "", elemToDoc = show) <+> "yield" <+> monoid(m) <+> show(e)
    case UnaryExp(op, e)            => unaryOp(op) <+> show(e)
    case FunAbs(idn, t, e)          => show(idn) <> ":" <+> tipe(t) <+> "=>" <+> show(e)
    case Gen(idn, e)                => show(idn) <+> "<-" <+> show(e)
    case Bind(idn, e)               => show(idn) <+> ":=" <+> show(e)
  }
}

/** CanonicalCalculusPrettyPrinter
  */
object CanonicalCalculusPrettyPrinter extends PrettyPrinter {

  import CanonicalCalculus._

  def pretty(n: Exp, w: Width): String =
    super.pretty(show(n), w=w)

  def gen(g: Gen): Doc = show(g.v) <+> "<-" <+> path(g.p)

  def path(p: Path): Doc = p match {
    case BoundVar(v)        => show(v)
    case ClassExtent(name)  => name
    case InnerPath(p, name) => path(p) <> dot <> name
  }

  def attr(att: AttrCons): Doc = att.idn <+> ":=" <+> show(att.e)

  def show(n: Exp): Doc = n match {
    case _: Null                    => "null"
    case c: Const                   => c.value.toString()
    case v: Var                     => "$var" + v.locn
    case RecordProj(e, idn)         => show(e) <> dot <> idn
    case RecordCons(atts)           => list(atts.toList, prefix = "", elemToDoc = attr)
    case IfThenElse(e1, e2, e3)     => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)      => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)    => collection(m, empty)
    case ConsCollectionMonoid(m, e) => collection(m, show(e))
    case MergeMonoid(m, e1, e2)     => show(e1) <+> merge(m) <+> show(e2)
    case Comp(m, paths, preds, e)   => "for" <+> parens(ssep((paths map gen) ++ (preds map show), comma <> space)) <+> "yield" <+> monoid(m) <+> show(e)
    case UnaryExp(op, e)            => unaryOp(op) <+> show(e)
  }
}
