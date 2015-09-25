package raw
package calculus

/** Errors
  */
sealed abstract class Error

case class MultipleDecl(i: Calculus.IdnNode) extends Error

case class UnknownDecl(i: Calculus.IdnNode) extends Error

case class UnknownPartition(p: Calculus.Partition) extends Error

case class CollectionRequired(t: Type) extends Error

// TODO: Make it take Exp not Types since only Exps need to be annotated once w/ positions by the parser

case class IncompatibleMonoids(m1: Monoid, t2: Type) extends Error

case class IncompatibleTypes(t1: Type, t2: Type) extends Error {
  override def toString() = s"incompatible types ${t1} (${t1.pos}) and ${t2} (${t2.pos})"
}

case class UnexpectedType(t: Type, expected: Type, desc: Option[String]) extends Error {
  override def toString() = s"incompatible types ${t} (${t.pos}) and ${expected}} (${expected.pos})"
}

/** ErrorPrettyPrinter
  */
object ErrorsPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(e: Error): String =
    super.pretty(show(e)).layout

  def show(e: Error): Doc = e match {
    case MultipleDecl(i) => s"${i.idn} is declared more than once (${i.pos})"
    case UnknownDecl(i) => s"${i.idn} is not declared (${i.pos})"
    case UnknownPartition(p) => s"partition is not declared as there is no SELECT with GROUP BY (${p.pos})"
    case CollectionRequired(t) => s"expected collection but got ${TypesPrettyPrinter(t)} (${t.pos})"
    case IncompatibleMonoids(m1, t2) => s"TODO m1 ${PrettyPrinter(m1)} t2 ${TypesPrettyPrinter(t2)}"
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

// TODO: Change signatures of Errors to take the positions when relevant: we now have the expression and constraint position to use.

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
    case ConstraintRecordType(atts, _) =>
      val satts = atts.map { case att => s"attribute ${att.idn} of type ${TypesPrettyPrinter(att.tipe)}" }.mkString(" and ")
      if (satts.nonEmpty)
        s"record with $satts"
      else
        s"record"
    case _ => PrettyPrinter(t)
  }
}
