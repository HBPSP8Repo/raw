package raw
package calculus

/** CalculusError
  */
sealed abstract class CalculusError extends RawError

case class MultipleDecl(i: Calculus.IdnNode, pos: Option[RawParserPosition] = None) extends CalculusError

case class UnknownDecl(i: Calculus.IdnNode, pos: Option[RawParserPosition] = None) extends CalculusError

case class AmbiguousIdn(idn: Calculus.IdnNode, pos: Option[RawParserPosition] = None) extends CalculusError

case class InvalidNumberOfArguments(nactual: Int, nexpected: Int, pos: Option[RawParserPosition] = None) extends CalculusError

case class InvalidArguments(pat: Calculus.Pattern, t: Type, pos: Option[RawParserPosition] = None) extends CalculusError

case class UnknownPartition(p: Calculus.Partition, pos: Option[RawParserPosition] = None) extends CalculusError

case class UnknownStar(s: Calculus.Star, pos: Option[RawParserPosition] = None) extends CalculusError

case class IncompatibleMonoids(m: Monoid, t: Type, pos: Option[RawParserPosition] = None) extends CalculusError

case class IncompatibleTypes(t1: Type, t2: Type, pos1: Option[RawParserPosition] = None, pos2: Option[RawParserPosition] = None) extends CalculusError

case class UnexpectedType(t: Type, expected: Set[Type], desc: Option[String] = None, pos: Option[RawParserPosition] = None) extends CalculusError

case class IllegalStar(s: Calculus.Select, pos: Option[RawParserPosition] = None) extends CalculusError

case class InvalidRegexSyntax(detail: String, pos: Option[RawParserPosition] = None) extends CalculusError

case class InvalidDateTimeFormatSyntax(detail: String, pos: Option[RawParserPosition] = None) extends CalculusError

case class InvalidFunction(t: Type, pos: Option[RawParserPosition] = None) extends CalculusError

/** ErrorPrettyPrinter
  */
object ErrorsPrettyPrinter extends org.kiama.output.PrettyPrinter {

  def apply(e: RawError): String =
    super.pretty(show(e)).layout

  def show(e: RawError): Doc = e match {
    case MultipleDecl(i, _) => s"${i.idn} is declared more than once"
    case UnknownDecl(i, _) => s"${i.idn} is not declared"
    case AmbiguousIdn(i, _) => s"${i.idn} is ambiguous"
    case InvalidNumberOfArguments(nactual, nexpected, _) => s"expected $nexpected arguments but got $nactual"
    case InvalidArguments(pat, t, _) => s"pattern $pat does not match expected type ${FriendlierPrettyPrinter(t)}}"
    case UnknownPartition(p, _) => s"partition is not declared as there is no SELECT with GROUP BY"
    case UnknownStar(s, _) => s"* is not declared as there is no SELECT or FOR"
    case IncompatibleMonoids(m, t, _) => s"${PrettyPrinter(m)} cannot be used with ${FriendlierPrettyPrinter(t)}"
    case IncompatibleTypes(t1, t2, _, _) => s"incompatible types: ${FriendlierPrettyPrinter(t1)} and ${FriendlierPrettyPrinter(t2)}"
    case UnexpectedType(t, expected, Some(desc), _) => s"expected $desc ${expected.map(FriendlierPrettyPrinter(_)).mkString(" or ")} but got ${FriendlierPrettyPrinter(t)}"
    case UnexpectedType(t, expected, None, _) => s"expected ${expected.map(FriendlierPrettyPrinter(_)).mkString(" or ")} but got ${FriendlierPrettyPrinter(t)}"
    case IllegalStar(s, _) => s"cannot use * together with other attributes in a projection without GROUP BY"
    case InvalidRegexSyntax(detail, _) => s"invalid regular expression: $detail"
    case InvalidDateTimeFormatSyntax(detail, _) => s"invalid date/time format string: $detail"
    case InvalidFunction(t, _) => s"expected function but got ${FriendlierPrettyPrinter(t)}"
  }
}

/** FriendlierPrettyPrinter
  * A more user-friendly representation of types, used for error reporting.
  */
object FriendlierPrettyPrinter extends PrettyPrinter {

  def apply(n: RawNode): String =
    super.pretty(show(n)).layout

  override def monoid(m: Monoid) = m match {
    // TODO!
    case _ => super.monoid(m)
  }

  override def tipe(t: Type) = t match {
    // TODO: Replace this by a more useful description of possible alternative types with the given properties (e.g. expected a bag or a list)
//    case CollectionType(m: MonoidVariable, innerType) =>
//      def mdesc(m: Monoid) = m match {
//        case m: GenericMonoid =>
//          val c = m.commutative match {
//            case Some(true)  => Seq("commutative")
//            case Some(false) => Seq("non-commutative")
//            case _           => Seq()
//          }
//          val i = m.idempotent match {
//            case Some(true)  => Seq("idempotent")
//            case Some(false) => Seq("non-idempotent")
//            case _           => Seq()
//          }
//          c ++ i
//      }
//      if (mdesc(m).nonEmpty)
//        monoid(m) <+> opt(t.nullable) <> "collection of" <+> tipe(innerType)
//      else
//        opt(t.nullable) <> "collection of" <+> tipe(innerType)
    // TODO: Put back nicer error message on incompatible types
//    case PartialRecordType(atts, _)                =>
//      val satts = atts.map { case att => s"attribute ${att.idn} of type ${FriendlierPrettyPrinter(att.tipe)}" }.mkString(" and ")
//      if (satts.nonEmpty)
//        s"record with $satts"
//      else
//        s"record"
    case _: NumberType => "number"
    case _                                            => PrettyPrinter(t)
  }

}
