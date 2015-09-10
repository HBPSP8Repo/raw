package raw
package calculus

import org.kiama.output.PrettyPrinterTypes.Width

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import scala.collection.immutable.Seq
  import Calculus._

  def apply(n: CalculusNode, w: Width = 120, debug: Option[PartialFunction[CalculusNode, String]] = None): String =
    super.pretty(show(n, debug), w=w).layout

  def isEscaped(s: String): Boolean =
    ("""[\t ]""".r findFirstIn(s)).isDefined

  def ident(idn: Idn): String =
    if (isEscaped(idn)) s"`$idn`" else idn

  def show(n: CalculusNode, debug: Option[PartialFunction[CalculusNode, String]]): Doc = {
    def apply(n: CalculusNode): Doc =
      (debug match {
        case Some(f) => if (f.isDefinedAt(n)) f(n) else ""
        case None    => ""
      }) <>
      (n match {
      case _: Null                    => "null"
      case BoolConst(v)               => v.toString
      case IntConst(v)                => v
      case FloatConst(v)              => v
      case StringConst(v)             => s""""$v""""
      case IdnDef(idn)                => idn
      case IdnUse(idn)                => ident(idn)
      case IdnExp(idn)                => apply(idn)
      case RecordProj(e, idn)         => apply(e) <> dot <> ident(idn)
      case AttrCons(idn, e)           => ident(idn) <+> ":=" <+> apply(e)
      case RecordCons(atts)           => parens(group(nest(lsep(atts.map(apply), comma))))
      case IfThenElse(e1, e2, e3)     => "if" <+> apply(e1) <+> "then" <+> apply(e2) <+> "else" <+> apply(e3)
      case BinaryExp(op, e1, e2)      => apply(e1) <+> binaryOp(op) <+> apply(e2)
      case FunApp(f, e)               => apply(f) <> parens(apply(e))
      case ZeroCollectionMonoid(m)    => collection(m, empty)
      case ConsCollectionMonoid(m, e) => collection(m, apply(e))
      case MergeMonoid(m, e1, e2)     => apply(e1) <+> merge(m) <+> apply(e2)
      case Comp(m, qs, e)             => "for" <+> parens(group(nest(lsep(qs.map(apply), ";")))) <+> "yield" <+> monoid(m) <+> apply(e)
      case UnaryExp(op, e)            => unaryOp(op) <+> apply(e)
      case FunAbs(p, e)               => "\\" <> apply(p) <+> "->" <+> apply(e)
      case Gen(p, e)                  => apply(p) <+> "<-" <+> apply(e)
      case Bind(p, e)                 => apply(p) <+> ":=" <+> apply(e)
      case ExpBlock(bs, e)            => val ns: Seq[CalculusNode] = bs :+ e; "{" <+> group(nest(lsep(ns.map(apply), ";"))) <+> "}"
      case PatternIdn(idn)            => apply(idn)
      case PatternProd(ps)            => parens(group(nest(lsep(ps.map(apply), comma))))

      case Reduce(m, e, p, child)         => softline <> "reduce" <> parens(nest(group(lsep(List(monoid(m), apply(e), apply(p), nest(apply(child))), comma))))
      case Nest(m, e, f, p, g, child)     => softline <> "nest" <> parens(nest(group(lsep(List(monoid(m), apply(e), apply(f), apply(p), apply(g), nest(apply(child))), comma))))
      case Filter(p, child)               => softline <> "filter" <> parens(nest(group(lsep(List(apply(p), nest(apply(child))), comma))))
      case Join(p, left, right)           => softline <> "join" <> parens(nest(group(lsep(List(apply(p), nest(apply(left)), nest(apply(right))), comma))))
      case Unnest(path, pred, child)      => softline <> "unnest" <> parens(nest(group(lsep(List(apply(path), apply(pred), nest(apply(child))), comma))))
      case OuterJoin(p, left, right)      => softline <> "outer_join" <> parens(nest(group(lsep(List(apply(p), nest(apply(left)), nest(apply(right))), comma))))
      case OuterUnnest(path, pred, child) => softline <> "outer_unnest" <> parens(group(nest(lsep(List(apply(path), apply(pred), nest(apply(child))), comma))))


        /*
        
/** Reduce
  */
case class Reduce(m: Monoid, e: Exp, p: Exp, child: Exp) extends LogicalAlgebraNode

/** Nest
  */
case class Nest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: Exp) extends LogicalAlgebraNode

/** Filter
  */
case class Filter(p: Exp, child: Exp) extends LogicalAlgebraNode

/** Join
  */
case class Join(p: Exp, left: Exp, right: Exp) extends LogicalAlgebraNode

/** Unnest
  */
case class Unnest(path: Exp, pred: Exp, child: Exp) extends LogicalAlgebraNode

/** OuterJoin
  */
case class OuterJoin(p: Exp, left: Exp, right: Exp) extends LogicalAlgebraNode

/** OuterUnnest
  */
case class OuterUnnest(path: Exp, pred: Exp, child: Exp) extends LogicalAlgebraNode
         */
    })

    apply(n)
  }
}