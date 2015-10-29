package raw
package regex

import com.typesafe.scalalogging.LazyLogging
import scala.util.parsing.combinator.RegexParsers

object RegexSyntaxAnalyzer extends RegexParsers with LazyLogging {

  import scala.collection.immutable.Seq
  import Regex._

  override def skipWhitespace = false

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String) = parseAll(regex, query) match {
    case Success(ast, _) => Right(ast)
    case error: NoSuccess => Left(error)
  }

  /** Parser combinators.
    */

  lazy val regex =
    positioned(rep(term0) ^^ { case rs => logger.debug("regex"); RegexSeq(rs) })

  lazy val term0: Parser[RegexNode] =
    positioned(
      ("(" ~> attrName) ~ (":" ~> term2) <~ ")" ^^ { case idn ~ r => RegexGroup(r, Some(idn))} |
      ("(" ~> term2 <~ ")" ^^ { case r => RegexGroup(r, None)}) |
      term25
  )

  lazy val term2 =
    positioned(rep1sep(term225, "|") ^^ { case rs => RegexPipe(rs) })

  lazy val term225 =
    term25 |
    "" ^^^ RegexSeq(Seq())

  lazy val term25 =
    positioned(rep1(term3) ^^ { case rs => logger.debug("cabum"); RegexSeq(rs) })

  lazy val term3 =
    plus |
    question |
    star |
    repeat |
    term4

  lazy val term4 =
    beginLine |
    endLine |
    or |
    word |
    space |
    digit |
    wildcard |
    char

  lazy val word =
    positioned("\\w" ^^^ RegexWord())

  lazy val space =
    positioned("\\s" ^^^ RegexSpace())

  lazy val digit =
    positioned("\\d" ^^^ RegexDigit())

  lazy val wildcard =
    positioned("." ^^^ RegexWildcard())

  lazy val char =
    positioned(
      "\\\\" ^^^ RegexEscapedChar('\\') |
      "\\[" ^^^ RegexEscapedChar('[') |
      "\\]" ^^^ RegexEscapedChar(']') |
      "\\(" ^^^ RegexEscapedChar('(') |
      "\\)" ^^^ RegexEscapedChar(')') |
      "\\{" ^^^ RegexEscapedChar('{') |
      "\\}" ^^^ RegexEscapedChar('}') |
      "\\*" ^^^ RegexEscapedChar('*') |
      "\\." ^^^ RegexEscapedChar('.') |
      "\\+" ^^^ RegexEscapedChar('+') |
      "\\?" ^^^ RegexEscapedChar('?') |
      "\\^" ^^^ RegexEscapedChar('^') |
      "\\$" ^^^ RegexEscapedChar('$') |
      """[^\\\[\](){}*\.+?^$]""".r ^^ { case c => logger.debug("char"); RegexChar(c.toCharArray.head) })

  lazy val plus =
    positioned(term4 <~ "+" ^^ { case r => RegexPlus(r) })

  lazy val question =
    positioned(term4 <~ "?" ^^ { case r => RegexQuestion(r) })

  lazy val star =
    positioned(term4 <~ "*" ^^ { case r => RegexStar(r) })

  lazy val repeat =
    positioned((term4 <~ "{") ~ ("\\d+".r <~ "}") ^^ { case r ~ d => RegexRepeat(r, d.toInt) })

  lazy val beginLine =
    positioned("^" ^^^ RegexBeginLine())

  lazy val endLine =
    positioned("$" ^^^ RegexEndLine())

  lazy val or =
    positioned(
      "[^" ~> rep1(orContent) <~ "]" ^^ { case ors => RegexOr(ors, true)} |
      "[" ~> rep1(orContent) <~ "]" ^^ { case ors => RegexOr(ors, false)})

  lazy val orContent =
    orInterval |
    orPrimitive

  lazy val orInterval =
    positioned(("\\w".r <~ "-") ~ "\\w".r ^^ {case a ~ b => RegexOrInterval(a.toCharArray.head, b.toCharArray.head)})

  lazy val orPrimitive =
    positioned(primitive ^^ { case p => RegexOrPrimitive(p) })

  lazy val primitive =
    word |
    space |
    digit |
    wildcard |
    primitiveChar

  lazy val primitiveChar =
    positioned(
      "\\\\" ^^^ RegexEscapedChar('\\') |
      "\\[" ^^^ RegexEscapedChar('[') |
      "\\]" ^^^ RegexEscapedChar(']') |
      "\\." ^^^ RegexEscapedChar('.') |
      """[^\\\[\]\.]""".r ^^ { case c => RegexChar(c.toCharArray.head) })

  lazy val attrName =
    escapedIdent |
    ident

  lazy val escapedIdent: Parser[String] =
    """`[\w\t ]+`""".r ^^ {case s => s.drop(1).dropRight(1) }

  // TODO: Block reserved words. Inherit from common parsing trait. All idents and reservedWords should go to the common trait, incl attrName
  lazy val ident =
    """[_a-zA-Z]\w*""".r
}
