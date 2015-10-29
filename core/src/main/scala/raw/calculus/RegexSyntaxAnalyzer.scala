package raw
package calculus

import scala.util.parsing.combinator.{RegexParsers, PackratParsers}
import scala.util.parsing.input.CharSequenceReader

object RegexSyntaxAnalyzer extends RegexParsers with PackratParsers {

  import Calculus._
  import scala.collection.immutable.Seq

  override val whiteSpace =
    ""

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[SyntaxAnalyzer.NoSuccess, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case error: NoSuccess => Left(error)
  }

  /** Work-around for parser combinator bug.
    */
  def parseSubstring[T](parser: Parser[T], input: String): ParseResult[T] = {
    parse(parser, new PackratReader(new CharSequenceReader(input)))
  }

  /** Parser combinators.
    */

  lazy val regex: PackratParser[RawRegex] =
    ???

  /*



slppp6.intermind.net - - [01/Aug/1995:00:00:12 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635
ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:12 -0400] "GET /history/apollo/images/apollo-logo1.gif HTTP/1.0" 200 1173
slppp6.intermind.net - - [01/Aug/1995:00:00:13 -0400] "GET /history/apollo/images/apollo-logo.gif HTTP/1.0" 200 3047

   */

}