package raw

/** Base class for calculus/canonical calculus pretty printers.
  */
abstract class PrettyPrinter extends org.kiama.output.PrettyPrinter {

  def unaryOp(op: UnaryOperator): Doc = op match {
    case _: Not        => "not"
    case _: Neg        => "-"
    case _: ToBool     => "to_bool"
    case _: ToInt      => "to_int"
    case _: ToFloat    => "to_float"
    case _: ToString   => "to_string"
    case _: ToBag      => "to_bag"
    case _: ToList     => "to_list"
    case _: ToSet      => "to_set"
    case _: IsNullOp   => "is" <+> "null"
    case _: IsNotNull  => "is" <+> "not" <+> "null"
  }

  def binaryOp(op: BinaryOperator): Doc = op match {
    case _: Eq  => "="
    case _: Neq => "<>"
    case _: Ge  => ">="
    case _: Gt  => ">"
    case _: Le  => "<="
    case _: Lt  => "<"
    case _: Plus => "+"
    case _: Sub => "-"
    case _: Mult => "*"
    case _: Div => "/"
    case _: Mod => "%"
    case _: And => "and"
    case _: Or => "or"
    case _: BagUnion => "bag_union"
    case _: Union => "union"
    case _: Append => "append"
    case _: Like => "like"
    case _: NotLike => "not" <+> "like"
    case _: In => "in"
    case _: NotIn => "not" <+> "in"
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
  }

  def monoid(m: Monoid): Doc = m match {
//    case MonoidVariable(leqMonoids, geqMonoids, sym) =>
//      val items = scala.collection.mutable.MutableList[Doc]()
//      if (leqMonoids.nonEmpty) {
//        items += parens(group(lsep(leqMonoids.map(shortMonoid).toList, comma)))
//      }
//      items += text(sym.idn)
//      if (geqMonoids.nonEmpty) {
//        items += parens(group(lsep(geqMonoids.map(shortMonoid).toList, comma)))
//      }
//      group(lsep(items.to, ":<"))
    case _ => shortMonoid(m)
  }

  def atts(recAtts: RecordAttributes): Doc = recAtts match {
    case Attributes(atts) => group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma)))
    case AttributesVariable(atts, sym) => group(nest(text(sym.idn) <> ":" <> lsep(atts.toList.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)) :+ text("..."), comma)))
    case ConcatAttributes(sym) => group(nest(text(sym.idn) <> ":" <> text("...")))
  }

  def collection(m: CollectionMonoid, d: Doc): Doc = monoid(m) <> parens(d)

  def tipe(t: Type): Doc = t match {
    case _: BoolType                          => "bool"
    case _: StringType                        => "string"
    case _: IntType                           => "int"
    case _: FloatType                         => "float"
    case OptionType(innerType)                => "option" <> parens(tipe(innerType))
    case RecordType(recAtts)                  => "record" <> parens(atts(recAtts))
    case CollectionType(m, innerType)         => monoid(m) <> brackets(tipe(innerType))
    case FunType(ins, out)                    => lsep(ins.map(tipe), comma) <+> "->" <+> tipe(out)
    case _: AnyType                           => "any"
    case _: NothingType                       => "nothing"
    case _: RegexType                         => "regex"
    case UserType(sym)                        => sym.idn
    case TypeScheme(t1, freeSyms) =>
      "type_scheme" <>
        parens(tipe(t1)) <>
        parens(group(nest(lsep(freeSyms.typeSyms.map { case sym => text(sym.idn) }.to, comma)))) <>
        parens(group(nest(lsep(freeSyms.monoidSyms.map { case sym => text(sym.idn) }.to, comma)))) <>
        parens(group(nest(lsep(freeSyms.attSyms.map { case sym => text(sym.idn) }.to, comma))))
    case NumberType(sym)                      => "number" <> parens(sym.idn)
    case TypeVariable(sym)                    => sym.idn
  }

  def show(n: RawNode): Doc = n match {
    case op: UnaryOperator  => unaryOp(op)
    case op: BinaryOperator => binaryOp(op)
    case m: Monoid          => monoid(m)
    case t: Type            => tipe(t)
    case a: RecordAttributes => atts(a)
  }
}

object PrettyPrinter extends PrettyPrinter {

  def apply(n: RawNode): String =
    super.pretty(show(n)).layout

}