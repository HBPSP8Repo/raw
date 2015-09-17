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
    case v: MonoidVariable => v.sym.idn
  }

  def collection(m: CollectionMonoid, d: Doc): Doc = monoid(m) <> parens(d)

  def opt(nullable: Boolean) = if (nullable) "?" else ""

  def tipe(t: Type): Doc = t match {

    case PrimitiveType(sym) => "primitive" <> parens(sym.idn)
    case NumberType(sym) => "number" <> parens(sym.idn)

    case _: BoolType   => opt(t.nullable) <> "bool"
    case _: StringType => opt(t.nullable) <> "string"
    case i: IntType    => opt(t.nullable) <> "int"
    case _: FloatType  => opt(t.nullable) <> "float"
    case RecordType(atts, Some(name)) =>
      opt(t.nullable) <> "record" <> parens(name) <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma))))
    case RecordType(atts, None) =>
      opt(t.nullable) <> "record" <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma))))
    case ConstraintRecordType(atts, sym) =>
      opt(t.nullable) <> "constraint_record" <> parens(sym.idn) <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)).to, comma))))
    case CollectionType(m, innerType)     => opt(t.nullable) <> monoid(m) <> parens(tipe(innerType))
    case FunType(p, e)          => opt(t.nullable) <> tipe(p) <+> "->" <+> tipe(e)
    case TypeVariable(sym)      => opt(t.nullable) <> sym.idn
    case _: AnyType             => opt(t.nullable) <> "any"
    case _: NothingType         => opt(t.nullable) <> "nothing"
    case UserType(sym)          => opt(t.nullable) <> sym.idn
    case TypeScheme(t1, vars)   => opt(t.nullable) <> "type_scheme" <> parens(tipe(t1)) <> parens(group(nest(lsep(vars.map{ case sym => text(sym.idn) }.to, comma))))
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