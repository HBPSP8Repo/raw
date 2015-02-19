package raw
package calculus

import org.kiama.output.PrettyPrinterTypes.Width

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import Calculus._

  def pretty(n: CalculusNode): String =
    super.pretty(show(n)).layout

  def pretty(n: CalculusNode, w: Width): String =
    super.pretty(show(n), w=w).layout

  def show(n: CalculusNode): Doc = n match {
    case _: Null                           => "null"
    case BoolConst(v)                      => v.toString
    case IntConst(v)                       => v
    case FloatConst(v)                     => v
    case StringConst(v)                    => s""""$v""""
    case IdnDef(idn)                       => idn
    case IdnUse(idn)                       => idn
    case IdnExp(idn)                       => show(idn)
    case RecordProj(e, idn)                => show(e) <> dot <> idn
    case AttrCons(idn, e)                  => idn <+> ":=" <+> show(e)
    case RecordCons(atts)                  => parens(group(nest(lsep(atts.map(show), comma))))
    case IfThenElse(e1, e2, e3)            => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)             => show(e1) <+> binaryOp(op) <+> show(e2)
    case FunApp(f, e)                      => show(f) <> parens(show(e))
    case ZeroCollectionMonoid(m)           => collection(m, empty)
    case ConsCollectionMonoid(m, e)        => collection(m, show(e))
    case MergeMonoid(m, e1, e2)            => show(e1) <+> merge(m) <+> show(e2)
    case Comp(m, qs, e)                    => "for" <+> parens(group(nest(lsep(qs.map(show), comma)))) <+> "yield" <+> monoid(m) <+> show(e)
    case UnaryExp(op, e)                   => unaryOp(op) <+> show(e)
    case FunAbs(idn, t, e)                 => show(idn) <> ":" <+> tipe(t) <+> "=>" <+> show(e)
    case Gen(idn, e)                       => show(idn) <+> "<-" <+> show(e)
    case Bind(idn, e)                      => show(idn) <+> ":=" <+> show(e)
  }
}