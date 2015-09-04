package raw
package calculus

/** Errors
  */
sealed abstract class Error

case class MultipleDecl(i: Calculus.IdnNode) extends Error

case class UnknownDecl(i: Calculus.IdnNode) extends Error

case class CollectionRequired(t: Type) extends Error

case class CommutativeIdempotentRequired(t: Type) extends Error

case class CommutativeRequired(t: Type) extends Error

case class IdempotentRequired(t: Type) extends Error

case class IncompatibleTypes(t1: Type, t2: Type) extends Error

case class UnexpectedType(t: Type, expected: Type, desc: Option[String]) extends Error

/** ErrorPrettyPrinter
  */
object ErrorsPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(e: Error): String =
    super.pretty(show(e)).layout

  def show(e: Error): Doc = e match {
    case MultipleDecl(i) => s"${i.idn} is declared more than once (${i.pos})"
    case UnknownDecl(i) => s"${i.idn} is not declared (${i.pos})"
    case CollectionRequired(t) => s"expected collection but got ${TypesPrettyPrinter(t)} (${t.pos})"
    case CommutativeIdempotentRequired(t) => s"expected commutative and idempotent collection but got non-commutative type ${TypesPrettyPrinter(t)} (${t.pos})"
    case CommutativeRequired(t) => s"expected commutative collection but got non-commutative type ${TypesPrettyPrinter(t)} (${t.pos})"
    case IdempotentRequired(t) => s"expected idempotent collection but got non-idempotent type ${TypesPrettyPrinter(t)} (${t.pos})"
    case IncompatibleTypes(t1, t2) =>
      if (t2.pos.line < t1.pos.line || (t1.pos.line == t2.pos.line && t2.pos.column < t1.pos.column))
        show(IncompatibleTypes(t2, t1))
      else
        (t1, t2) match {
          case (RecordType(_, name1), RecordType(_, name2)) if name1 != name2 =>
            s"records from different sources"
          case (RecordType(atts1, _), RecordType(atts2, _)) if atts1.length != atts2.length || atts1.map(_.idn) != atts2.map(_.idn) =>
            s"records with different attributes"
          case _ =>
            s"incompatible types: ${TypesPrettyPrinter(t1)} (${t1.pos}) and ${TypesPrettyPrinter(t2)} (${t2.pos})"
        }
    case UnexpectedType(t, _, Some(desc)) =>
      s"$desc but got ${TypesPrettyPrinter(t)} (${t.pos})"
    case UnexpectedType(t, expected, None) =>
      s"expected ${TypesPrettyPrinter(expected)} but got ${TypesPrettyPrinter(t)} (${t.pos})"
  }
}

/** TypesPrettyPrinter
  * A more user-friendly representation of types, used for error reporting.
  */
object TypesPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(t: Type): String =
    super.pretty(show(t)).layout

  private def p(v: Option[Boolean], s: String) = v match {
    case Some(true) => Some(s)
    case Some(false) => Some(s"non-$s")
    case _ => None
  }

  def show(t: Type): Doc = t match {
    case _: NumberType => "number"
    case _: PrimitiveType => "primitive"
    case ConstraintCollectionType(inner, c, i, _) =>
      val prefix = List(p(c, "commutative"), p(i, "idempotent")).filter(_.isDefined).mkString(" and ")
      if (prefix.nonEmpty)
        prefix + " collection of " + TypesPrettyPrinter(inner)
      else
        "collection of " + TypesPrettyPrinter(inner)
    case ConstraintRecordType(atts, _) =>
      val satts = atts.map { case att => s"attribute ${att.idn} of type ${TypesPrettyPrinter(att.tipe)}" }.mkString(" and ")
      if (satts.nonEmpty)
        s"record with $satts"
      else
        s"record"
    case FunType(p, e, _) =>
      // TODO: Pretty print the FunType constraints?
      s"function taking ${TypesPrettyPrinter(p)} and returning ${TypesPrettyPrinter(e)}"
    case v: TypeVariable => v.sym.idn // TODO: or should be any?
    case _ => PrettyPrinter(t)
  }
}
