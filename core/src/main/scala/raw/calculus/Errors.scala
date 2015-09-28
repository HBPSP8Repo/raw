package raw
package calculus

import scala.util.parsing.input.Position

/** Errors
  */
sealed abstract class Error

case class MultipleDecl(i: Calculus.IdnNode) extends Error

case class UnknownDecl(i: Calculus.IdnNode) extends Error

case class UnknownPartition(p: Calculus.Partition) extends Error

case class CollectionRequired(t: Type, p: Option[Position] = None) extends Error

case class IncompatibleMonoids(m: Monoid, t: Type, p: Option[Position] = None) extends Error

case class IncompatibleTypes(t1: Type, t2: Type, p1: Option[Position] = None, p2: Option[Position] = None) extends Error

case class UnexpectedType(t: Type, expected: Type, desc: Option[String] = None, p: Option[Position] = None) extends Error

/** ErrorPrettyPrinter
  */
object ErrorsPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(e: Error): String =
    super.pretty(show(e)).layout

  def show(e: Error): Doc = e match {
    case MultipleDecl(i) => s"${i.idn} is declared more than once (${i.pos})"
    case UnknownDecl(i) => s"${i.idn} is not declared (${i.pos})"
    case UnknownPartition(p) => s"partition is not declared as there is no SELECT with GROUP BY (${p.pos})"
    case CollectionRequired(t, Some(p)) => s"expected collection but got ${FriendlierPrettyPrinter(t)} ($p)"
    case IncompatibleMonoids(m, t, Some(p)) => s"incompatible monoids: ${PrettyPrinter(m)} (${m.pos}) with ${FriendlierPrettyPrinter(t)} ($p)"
    case IncompatibleTypes(t1, t2, Some(p1), Some(p2)) =>
      if (p2.line < p1.line || (p1.line == p2.line && p2.column < p1.column))
        show(IncompatibleTypes(t2, t1, Some(p2), Some(p1)))
      else
        (t1, t2) match {
          case (RecordType(_, name1), RecordType(_, name2)) if name1 != name2 =>
            s"records from different sources"
          case (RecordType(atts1, _), RecordType(atts2, _)) if atts1.length != atts2.length || atts1.map(_.idn) != atts2.map(_.idn) =>
            s"records with different attributes"
          case _ =>
            s"incompatible types: ${FriendlierPrettyPrinter(t1)} ($p1) and ${FriendlierPrettyPrinter(t2)} ($p2)"
        }
    case UnexpectedType(t, _, Some(desc), Some(p)) =>
      s"$desc but got ${FriendlierPrettyPrinter(t)} ($p)"
    case UnexpectedType(t, expected, None, Some(p)) =>
      s"expected ${FriendlierPrettyPrinter(expected)} but got ${FriendlierPrettyPrinter(t)} ($p)"
  }
}

/** FriendlierPrettyPrinter
  * A more user-friendly representation of types, used for error reporting.
  */
object FriendlierPrettyPrinter extends PrettyPrinter {

  def apply(n: RawNode): String =
    super.pretty(show(n)).layout

  override def monoid(m: Monoid) = m match {
    case MonoidVariable(c, i, _) =>
      val c = m.commutative match {
        case Some(true) => Seq("commutative")
        case Some(false) => Seq("non-commutative")
        case _ => Seq()
      }
      val i = m.idempotent match {
        case Some(true)  => Seq("idempotent")
        case Some(false) => Seq("non-idempotent")
        case _           => Seq()
      }
      (c ++ i).mkString(" and ")
    case _ => super.monoid(m)
  }

  override def tipe(t: Type) = t match {
      case CollectionType(m: MonoidVariable, innerType) =>
        opt(t.nullable) <> monoid(m) <> "collection of" <+> parens(tipe(innerType))
    case ConstraintRecordType(atts, _) =>
      val satts = atts.map { case att => s"attribute ${att.idn} of type ${FriendlierPrettyPrinter(att.tipe)}" }.mkString(" and ")
      if (satts.nonEmpty)
        s"record with $satts"
      else
        s"record"
    case _ => PrettyPrinter(t)
  }
}
