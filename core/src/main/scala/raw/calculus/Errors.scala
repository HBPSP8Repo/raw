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

case object TooManySolutions extends Error

/** ErrorPrettyPrinter
  */
object ErrorsPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(e: Error): String =
    super.pretty(show(e)).layout

  def show(e: Error): Doc = e match {
    case MultipleDecl(i) => s"$i is declared more than once (${i.idn})"
    case UnknownDecl(i) => s"$i is not declared (${i.idn})"
    case CollectionRequired(t) => s"expected collection but got ${TypesPrettyPrinter(t)}} (${t.pos})"
    case CommutativeIdempotentRequired(t) => s"expected commutative and idempotent collection but got non-commutative type ${TypesPrettyPrinter(t)}} (${t.pos})"
    case CommutativeRequired(t) => s"expected commutative collection but got non-commutative type ${TypesPrettyPrinter(t)}} (${t.pos})"
    case IdempotentRequired(t) => s"expected idempotent collection but got non-idempotent type ${TypesPrettyPrinter(t)}} (${t.pos})"
    case IncompatibleTypes(t1, t2) =>
      s"incompatible types: ${TypesPrettyPrinter(t1)}} (${t1.pos}) and ${TypesPrettyPrinter(t2)}} (${t2.pos})"
    case UnexpectedType(t, _, Some(desc)) =>
      s"$desc but got ${TypesPrettyPrinter(t)}} (${t.pos})"
    case UnexpectedType(t, expected, None) =>
      s"expected ${TypesPrettyPrinter(expected)} but got ${TypesPrettyPrinter(t)}} (${t.pos})"
    case TooManySolutions =>
      s"could not type check: too many solutions found"
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
    case ConstraintCollectionType(_, inner, c, i) =>
      val prefix = List(p(c, "commutative"), p(i, "idempotent")).filter(_.isDefined).mkString(" and ")
      if (prefix.nonEmpty)
        prefix + " collection of " + TypesPrettyPrinter(inner)
      else
        "collection of " + TypesPrettyPrinter(inner)
    case ConstraintRecordType(idn, atts) =>
      val satts = atts.map { case att => s"attribute ${att.idn} of type ${TypesPrettyPrinter(att.tipe)}" }.mkString(" and ")
      if (satts.nonEmpty)
        s"record with $satts"
      else
        s"record"
    case FunType(t1, t2) =>
      s"function taking ${TypesPrettyPrinter(t1)} and returning ${TypesPrettyPrinter(t2)}"
    case _: TypeVariable => "unknown" // TODO: or should be any?
    case _ => PrettyPrinter(t)
  }
}
