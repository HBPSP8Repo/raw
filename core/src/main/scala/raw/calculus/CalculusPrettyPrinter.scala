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
      case IdnDef(idn, Some(t))       => idn <+> ":" <+> tipe(t)
      case IdnDef(idn, None)          => ident(idn)
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
      case FunAbs(idn, e)             => "\\" <> apply(idn) <+> "->" <+> apply(e)
      case Gen(idn, e)                => apply(idn) <+> "<-" <+> apply(e)
      case Bind(idn, e)               => apply(idn) <+> ":=" <+> apply(e)
      case ExpBlock(bs, e)            => val ns: Seq[CalculusNode] = bs :+ e; "{" <+> group(nest(lsep(ns.map(apply), ";"))) <+> "}"
      case PatternIdn(idn)            => apply(idn)
      case PatternProd(ps)            => parens(group(nest(lsep(ps.map(apply), comma))))
      case PatternBind(p, e)          => apply(p) <+> ":=" <+> apply(e)
      case PatternGen(p, e)           => apply(p) <+> "<-" <+> apply(e)
      case PatternFunAbs(p, e)        => "\\" <> apply(p) <+> "->" <+> apply(e)
    })

    apply(n)
  }
}