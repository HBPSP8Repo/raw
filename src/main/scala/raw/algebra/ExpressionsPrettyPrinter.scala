package raw
package algebra

object ExpressionsPrettyPrinter extends PrettyPrinter {

  import Expressions._

  def apply(n: Exp): String =
    super.pretty(show(n)).layout

  def show(e: Exp): Doc = e match {
    case Null                           => "null"
    case BoolConst(v)                   => v.toString
    case IntConst(v)                    => v
    case FloatConst(v)                  => v
    case StringConst(v)                 => s""""$v""""
    case Arg                            => "$$arg"
    case RecordProj(e1, idn)            => show(e1) <> dot <> idn
    case RecordCons(atts)               => parens(group(nest(lsep(atts.map{ att => att.idn <+> ":=" <+> show(att.e) }, comma))))
    case IfThenElse(e1, e2, e3)         => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)          => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)        => collection(m, empty)
    case ConsCollectionMonoid(m, e1)    => collection(m, show(e1))
    case MergeMonoid(m, e1, e2)         => show(e1) <+> merge(m) <+> show(e2)
    case UnaryExp(op, e1)               => unaryOp(op) <+> show(e1)
  }
}
