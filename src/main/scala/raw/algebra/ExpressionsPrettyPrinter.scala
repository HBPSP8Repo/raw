package raw
package algebra

import org.kiama.output.PrettyPrinter.Doc

object ExpressionsPrettyPrinter extends PrettyPrinter {
  import Expressions._

  def pretty(e: ExpNode): String =
    super.pretty(show(e))

  def show(e: ExpNode): Doc = e match {
    case Null                       => "null"
    case StringConst(v)             => s""""$v""""
    case c: Const                   => c.value.toString()
    case IdnDef(idn)                => idn
    case IdnUse(idn)                => idn
    case IdnExp(idn)                => show(idn)
    case RecordProj(e, idn)         => show(e) <> dot <> idn
    case AttrCons(idn, e)           => idn <+> ":=" <+> show(e)
    case RecordCons(atts)           => list(atts.toList, prefix = "", elemToDoc = show)
    case IfThenElse(e1, e2, e3)     => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)      => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)    => collection(m, empty)
    case ConsCollectionMonoid(m, e) => collection(m, show(e))
    case MergeMonoid(m, e1, e2)     => show(e1) <+> merge(m) <+> show(e2)
    case UnaryExp(op, e)            => unaryOp(op) <+> show(e)
  }
}