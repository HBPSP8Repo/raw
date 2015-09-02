package raw

/** Base class for calculus/canonical calculus pretty printers.
  */
abstract class PrettyPrinter extends org.kiama.output.PrettyPrinter {

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
    case _: Mod => "%"
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
    case _: MinMonoid      => "min"
    case _: MultiplyMonoid => "multiply"
    case _: SumMonoid      => "sum"
    case _: AndMonoid      => "and"
    case _: OrMonoid       => "or"
    case _: BagMonoid      => "bag"
    case _: SetMonoid      => "set"
    case _: ListMonoid     => "list"
  }

  def collection(m: CollectionMonoid, d: Doc): Doc = monoid(m) <> parens(d)

  def tipe(t: Type): Doc = t match {
    case _: BoolType   => "bool"
    case _: StringType => "string"
    case _: IntType    => "int"
    case _: FloatType  => "float"
    case RecordType(atts, Some(name)) =>
      "record" <> parens(name) <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma))))
    case RecordType(atts, None) =>
      "record" <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma))))
    case ConstraintRecordType(atts, sym) =>
      "constraint_record" <> parens(sym.idn) <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)).to, comma))))
    case ConstraintCollectionType(innerType, c, i, sym) =>
      "constraint_collection" <> parens(sym.idn) <> parens(tipe(innerType) <+> c.toString <+> i.toString)
    case BagType(innerType)     => "bag" <> parens(tipe(innerType))
    case ListType(innerType)    => "list" <> parens(tipe(innerType))
    case SetType(innerType)     => "set" <> parens(tipe(innerType))
    case FunType(t1, t2)        => tipe(t1) <+> "->" <+> tipe(t2)
    case TypeVariable(sym)      => s"<${sym.idn}>"
    case _: AnyType             => "any"
    case _: NothingType         => "nothing"
  }

}

object PrettyPrinter extends PrettyPrinter {

  def apply(n: RawNode): String =
    super.pretty(show(n)).layout

  def show(n: RawNode): Doc = n match {
    case op: UnaryOperator  => unaryOp(op)
    case op: BinaryOperator => binaryOp(op)
    case m: Monoid          => monoid(m)
    case t: Type            => tipe(t)
  }
}