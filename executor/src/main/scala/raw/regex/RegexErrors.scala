package raw
package regex

sealed abstract class RegexError extends RawError

case class InvalidRegex(desc: String) extends RegexError
