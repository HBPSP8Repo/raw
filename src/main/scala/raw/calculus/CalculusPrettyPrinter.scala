package raw
package calculus

import org.kiama.output.PrettyPrinterTypes.Width

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import Calculus._

  def apply(n: CalculusNode, w: Width = 120, debug: Option[PartialFunction[CalculusNode, String]] = None): String =
    super.pretty(show(n, debug), w=w).layout

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
      case IdnUse(idn)                => idn
      case IdnExp(idn)                => apply(idn)
      case RecordProj(e, idn)         => apply(e) <> dot <> idn
      case AttrCons(idn, e)           => idn <+> ":=" <+> apply(e)
      case RecordCons(atts)           => parens(group(nest(lsep(atts.map(apply), comma))))
      case IfThenElse(e1, e2, e3)     => "if" <+> apply(e1) <+> "then" <+> apply(e2) <+> "else" <+> apply(e3)
      case BinaryExp(op, e1, e2)      => apply(e1) <+> binaryOp(op) <+> apply(e2)
      case FunApp(f, e)               => apply(f) <> parens(apply(e))
      case ZeroCollectionMonoid(m)    => collection(m, empty)
      case ConsCollectionMonoid(m, e) => collection(m, apply(e))
      case MergeMonoid(m, e1, e2)     => apply(e1) <+> merge(m) <+> apply(e2)
      case Comp(m, qs, e)             => "for" <+> parens(group(nest(lsep(qs.map(apply), comma)))) <+> "yield" <+> monoid(m) <+> apply(e)
      case UnaryExp(op, e)            => unaryOp(op) <+> apply(e)
      case FunAbs(idn, t, e)          => apply(idn) <> ":" <+> tipe(t) <+> "=>" <+> apply(e)
      case Gen(idn, e)                => apply(idn) <+> "<-" <+> apply(e)
      case Bind(idn, e)               => apply(idn) <+> ":=" <+> apply(e)
    })

    apply(n)
  }
}