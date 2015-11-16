package raw
package calculus

import org.kiama.output.PrettyPrinterTypes.Width

/** CalculusPrettyPrinter
  */
object CalculusPrettyPrinter extends PrettyPrinter {

  import Calculus._

  import scala.collection.immutable.Seq

  def apply(n: CalculusNode, w: Width = 120, debug: Option[PartialFunction[CalculusNode, String]] = None): String =
    super.pretty(show(n, debug), w=w).layout

  def isEscaped(s: String): Boolean =
    ("""[\t ]""".r findFirstIn(s)).isDefined

  def ident(idn: Idn): String =
    if (isEscaped(idn)) s"`$idn`" else idn

  private def descapeStr(s: String) = {
    var descapedStr = ""
    for (c <- s) {
      descapedStr += (c match {
        case '\\' => "\\\\"
        case '\'' => "\\'"
        case '\"' => "\\\""
        case '\b' => "\\b"
        case '\f' => "\\f"
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case _ => c
      })
    }
    descapedStr
  }

  private def parseProperties(p: ParseProperties) = p match {
    case _: SkipOnFail => "skip" <+> "on" <+> "fail"
    case _: NullOnFail => "null" <+> "on" <+> "fail"
  }

  // TODO: Use same constants as in SyntaxAnalyzer (e.g. kwNull, kwIf, kwAs, etc)
  def show(n: CalculusNode, debug: Option[PartialFunction[CalculusNode, String]]): Doc = {
    def apply(n: CalculusNode): Doc =
      (debug match {
        case Some(f) => if (f.isDefinedAt(n)) f(n) else ""
        case None    => ""
      }) <>
      group(n match {
        case _: Null                        => "null"
        case BoolConst(v)                   => v.toString
        case IntConst(v)                    => v
        case FloatConst(v)                  => v
        case StringConst(v)                 => s""""${descapeStr(v)}""""
        case RegexConst(v)                  => s"""r"${descapeStr(v)}""""
        case IdnDef(idn, Some(t))           => idn <> ":" <> tipe(t)
        case IdnDef(idn, None)              => idn
        case IdnUse(idn)                    => ident(idn)
        case IdnExp(idn)                    => apply(idn)
        case RecordProj(e, idn)             => apply(e) <> dot <> ident(idn)
        case AttrCons(idn, e)               => ident(idn) <> ":" <+> apply(e)
        case RecordCons(atts)               => parens(group(nest(lsep(atts.map(apply), comma))))
        case IfThenElse(e1, e2, e3)         => "if" <+> apply(e1) <+> "then" <+> apply(e2) <+> "else" <+> apply(e3)
        case BinaryExp(op, e1, e2)          => apply(e1) <+> binaryOp(op) <+> apply(e2)
        case FunApp(f, args)                => apply(f) <> parens(lsep(args.map(apply), comma))
        case ZeroCollectionMonoid(m)        => collection(m, empty)
        case ConsCollectionMonoid(m, e)     => collection(m, apply(e))
        case MultiCons(m, es)               => collection(m, group(lsep(es.map(apply), ",")))
        case Comp(m, qs, e)                 => "for" <+> parens(group(nest(lsep(qs.map(apply), ";")))) <+> "yield" <+> monoid(m) <+> apply(e)
        case UnaryExp(op @ (_ : Not | _: Neg), e) =>
          unaryOp(op) <+> apply(e)
        case UnaryExp(op @ (_: IsNullOp | _: IsNotNull), e) =>
          apply(e) <+> unaryOp(op)
        case UnaryExp(op, e)                => unaryOp(op) <> parens(apply(e))
        case FunAbs(args, e)                => "\\" <> lsep(args.map(apply), comma) <+> "->" <+> apply(e)
        case Gen(Some(p), e)                => apply(p)  <+> "<-" <+> apply(e)
        case Gen(None, e)                   => "<-" <+> apply(e)
        case Bind(p, e)                     => apply(p) <+> ":=" <+> apply(e)
        case ExpBlock(bs, e)                => val ns: Seq[CalculusNode] = bs :+ e; "{" <+> group(nest(lsep(ns.map(apply), ";"))) <+> "}"
        case Sum(e)                         => "sum" <> parens(apply(e))
        case Max(e)                         => "max" <> parens(apply(e))
        case Min(e)                         => "min" <> parens(apply(e))
        case Avg(e)                         => "avg" <> parens(apply(e))
        case Count(e)                       => "count" <> parens(apply(e))
        case Exists(e)                      => "exists" <> parens(apply(e))
        case PatternIdn(idn)                => apply(idn)
        case PatternProd(ps)                => parens(group(nest(lsep(ps.map(apply), comma))))
        case CanComp(m, gs, ps, e)          => "for" <+> parens(group(nest(lsep((gs ++ ps).map(apply), ";")))) <+> "yield" <+> monoid(m) <+> apply(e)
        case Reduce(m, child, e)            => "reduce" <> parens(nest(group(lsep(List(monoid(m), nest(apply(child)), apply(e)), comma))))
        case Nest(m, child, k, p, e)        => "nest" <> parens(nest(group(lsep(List(monoid(m), nest(apply(child)), apply(k), apply(p), apply(e)), comma))))
        case Nest2(m, child, k, p, e)       => "nest2" <> parens(nest(group(lsep(List(monoid(m), nest(apply(child)), apply(k), apply(p), apply(e)), comma))))
        case Filter(child, p)               => "filter" <> parens(nest(group(lsep(List(nest(apply(child)), apply(p)), comma))))
        case Join(left, right, p)           => "join" <> parens(nest(group(lsep(List(nest(apply(left)), nest(apply(right)), apply(p)), comma))))
        case OuterJoin(left, right, p)      => "outer_join" <> parens(nest(group(lsep(List(nest(apply(left)), nest(apply(right)), apply(p)), comma))))
        case Unnest(child, path, pred)      => "unnest" <> parens(nest(group(lsep(List(nest(apply(child)), apply(path), apply(pred)), comma))))
        case OuterUnnest(child, path, pred) => "outer_unnest" <> parens(group(nest(lsep(List(nest(apply(child)), apply(path), apply(pred)), comma))))
        case Partition()                    => "partition"
        case Star()                         => "*"
        case Select(f, d, g, proj, w, o, h) =>
          (if (d) ("select distinct") else "select") <+> apply(proj) <+>
            lsep(List(
              group("from" <+> lsep(f.map(apply), comma)),
              group((if (w.isDefined) "where" <+> apply(w.get) else "")),
              group((if (g.isDefined) "group by" <+> apply(g.get) else "")),
              group((if (o.isDefined) "order" <+> apply(o.get) else "")),
              group((if (h.isDefined) "having" <+> apply(h.get) else ""))), space)
        case Into(e1, e2)                   => apply(e1) <+> "into" <+> apply(e2)
        case ParseAs(e, r, Some(p))                  => show(e, debug) <+> "parse" <+> "as" <+> "r" <> dquote <> r.value <> dquote <+> parseProperties(p)
        case ParseAs(e, r, None)                  => show(e, debug) <+> "parse" <+> "as" <+> "r" <> dquote <> r.value <> dquote
        case ToEpoch(e, fmt)                => "to_epoch" <> parens(group(lsep(List(show(e, debug), dquotes(fmt)), comma)))
        case IsNull(e, or)                  => "isNull" <> parens(group(lsep(List(show(e, debug), show(or, debug)), comma)))
      }
      )

    apply(n)
  }
}