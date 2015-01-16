package raw.calculus

/** Base class for calculus/canonical calculus pretty printers.
  */
class PrettyPrinter extends org.kiama.output.PrettyPrinter {

  def unaryOp(op: UnaryOperator) = op match {
    case _: Not      => "not"
    case _: Neg      => "-"
    case _: ToBool   => "to_bool"
    case _: ToInt    => "to_int"
    case _: ToFloat  => "to_float"
    case _: ToString => "to_string"
  }

  def binaryOp(op: BinaryOperator): Doc = op match {
    case _: Eq  => "="
    case _: Neq => "<>"
    case _: Ge  => ">="
    case _: Gt  => ">"
    case _: Le  => "<="
    case _: Lt  => "<"
    case _: Sub => "-"
    case _: Div => "/"
  }

  def merge(m: Monoid): Doc = m match {
    case _: MaxMonoid      => "max"
    case _: MultiplyMonoid => "*"
    case _: SumMonoid      => "+"
    case _: AndMonoid      => "and"
    case _: OrMonoid       => "or"
    case _: BagMonoid      => "bag_union"
    case _: SetMonoid      => "union"
    case _: ListMonoid     => "append"
  }

  def monoid(m: Monoid): Doc = m match {
    case _: MaxMonoid      => "max"
    case _: MultiplyMonoid => "multiply"
    case _: SumMonoid      => "sum"
    case _: AndMonoid      => "and"
    case _: OrMonoid       => "or"
    case _: BagMonoid      => "bag"
    case _: SetMonoid      => "set"
    case _: ListMonoid     => "list"
  }

  def collection(m: CollectionMonoid, d: Doc): Doc = m match {
    case _: BagMonoid  => "bag{" <+> d <+> "}"
    case _: SetMonoid  => "{" <+> d <+> "}"
    case _: ListMonoid => "[" <+> d <+> "]"
  }

  def tipe(t: Type): Doc = t match {
    case _: BoolType                  => "bool"
    case _: StringType                => "string"
    case _: FloatType                 => "float"
    case _: IntType                   => "int"
    case RecordType(atts)             => "record" <> list(atts.toList, prefix = "", elemToDoc = (att: AttrType) => att.idn <+> "=" <+> tipe(att.tipe))
    case CollectionType(m, innerType) => monoid(m) <> parens(tipe(innerType))
    case ClassType(idn)               => idn
    case FunType(t1, t2)              => tipe(t1) <+> "->" <+> tipe(t2)
    case _: NoType                    => "none"
    case _: UnknownType               => "unknown"
  }

}

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import Calculus._

  def pretty(n: CalculusNode, w: Width): String =
    super.pretty(show(n), w=w)

  def show(n: CalculusNode): Doc = n match {
    case _: Null                    => "null"
    case BoolConst(v)               => v.toString()
    case IntConst(v)                => v.toString()
    case FloatConst(v)              => v.toString()
    case StringConst(v)             => v
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

  def pretty(n: CalculusNode, w: Width): String =
    super.pretty(show(n), w=w)

  def path(p: Path): Doc = p match {
    case BoundVar(v)        => show(v)
    case ClassExtent(name)  => name
    case InnerPath(p, name) => path(p) <> dot <> name
  }

  def show(n: CalculusNode): Doc = n match {
    case _: Null                    => "null"
    case BoolConst(v)               => v.toString()
    case IntConst(v)                => v.toString()
    case FloatConst(v)              => v.toString()
    case StringConst(v)             => v
    case v: Var                     => "$var" + v.locn
    case RecordProj(e, idn)         => show(e) <> dot <> idn
    case AttrCons(idn, e)           => idn <+> ":=" <+> show(e)
    case RecordCons(atts)           => list(atts.toList, prefix = "", elemToDoc = show)
    case IfThenElse(e1, e2, e3)     => "if" <+> show(e1) <+> "then" <+> show(e2) <+> "else" <+> show(e3)
    case BinaryExp(op, e1, e2)      => show(e1) <+> binaryOp(op) <+> show(e2)
    case ZeroCollectionMonoid(m)    => collection(m, empty)
    case ConsCollectionMonoid(m, e) => collection(m, show(e))
    case MergeMonoid(m, e1, e2)     => show(e1) <+> merge(m) <+> show(e2)
    case Comp(m, paths, preds, e)   => "for" <+> parens(ssep(paths ++ preds map show, comma <> space)) <+> "yield" <+> monoid(m) <+> show(e)
    case UnaryExp(op, e)            => unaryOp(op) <+> show(e)
    case Gen(v, p)                  => show(v) <+> "<-" <+> path(p)
  }
}
