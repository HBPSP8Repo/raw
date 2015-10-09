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
    case _: ToBag    => "to_bag"
    case _: ToList   => "to_list"
    case _: ToSet    => "to_set"
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

  def shortMonoid(m: Monoid): Doc = m match {
    case _: MaxMonoid           => "max"
    case _: MinMonoid           => "min"
    case _: MultiplyMonoid      => "multiply"
    case _: SumMonoid           => "sum"
    case _: AndMonoid           => "and"
    case _: OrMonoid            => "or"
    case _: BagMonoid           => "bag"
    case _: SetMonoid           => "set"
    case _: ListMonoid          => "list"
    case mv: MonoidVariable => mv.sym.idn

    case GenericMonoid(commutative, idempotent) => "generic" <> parens(group(lsep(List(
      text(if (commutative.isDefined) commutative.get.toString else "?"),
      text(if (idempotent.isDefined) idempotent.get.toString else "?")), comma)))
  }

  def monoid(m: Monoid): Doc = m match {
    case MonoidVariable(leqMonoids, geqMonoids, sym) =>
      val items = scala.collection.mutable.MutableList[Doc]()
      if (leqMonoids.nonEmpty) {
        items += parens(group(lsep(leqMonoids.map(shortMonoid).toList, comma)))
      }
      items += text(sym.idn)
      if (geqMonoids.nonEmpty) {
        items += parens(group(lsep(geqMonoids.map(shortMonoid).toList, comma)))
      }
      group(lsep(items.to, ":<"))
    case _ => shortMonoid(m)
  }

  def atts(recAtts: RecordAttributes): Doc = recAtts match {
    case Attributes(atts) => group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma)))
    case AttributesVariable(atts, sym) => group(nest(text(sym.idn) <> ":" <> lsep(atts.toList.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)) :+ text("..."), comma)))
    case ConcatAttributes(atts, sym) => group(nest(text(sym.idn) <> ":" <> lsep(atts.toList.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)) :+ text("..."), comma)))
  }

  def collection(m: CollectionMonoid, d: Doc): Doc = monoid(m) <> parens(d)

  def opt(nullable: Boolean) = if (nullable) "?" else ""

  def tipe(t: Type): Doc = opt(t.nullable) <> (t match {
    case _: BoolType                          => "bool"
    case _: StringType                        => "string"
    case _: IntType                           => "int"
    case _: FloatType                         => "float"
    case RecordType(recAtts)                  => "record" <> parens(atts(recAtts))
    case PatternType(pAtts)                   => "pattern" <> parens(group(lsep(pAtts.map { case att => tipe(att.tipe) }, comma)))
    case CollectionType(m, innerType)         => monoid(m) <> brackets(tipe(innerType))
    case FunType(p, e)                        => tipe(p) <+> "->" <+> tipe(e)
    case _: AnyType                           => "any"
    case _: NothingType                       => "nothing"
    case UserType(sym)                        => sym.idn
    case TypeScheme(t1, typeSyms, monoidSyms, attSyms) =>
      "type_scheme" <>
        parens(tipe(t1)) <>
        parens(group(nest(lsep(typeSyms.map { case sym => text(sym.idn) }.to, comma)))) <>
        parens(group(nest(lsep(monoidSyms.map { case sym => text(sym.idn) }.to, comma)))) <>
        parens(group(nest(lsep(attSyms.map { case sym => text(sym.idn) }.to, comma))))
    case PrimitiveType(sym)                   => "primitive" <> parens(sym.idn)
    case NumberType(sym)                      => "number" <> parens(sym.idn)
    case TypeVariable(sym)                    => sym.idn
  })

  def show(n: RawNode): Doc = n match {
    case op: UnaryOperator  => unaryOp(op)
    case op: BinaryOperator => binaryOp(op)
    case m: Monoid          => monoid(m)
    case t: Type            => tipe(t)
  }
}

object PrettyPrinter extends PrettyPrinter {

  def apply(n: RawNode): String =
    super.pretty(show(n)).layout

}