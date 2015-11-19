package raw
package calculus

sealed abstract class Warning

case class ImplicitConversion(orig: Type, n: Type, pos: Option[RawParserPosition] = None) extends Warning
