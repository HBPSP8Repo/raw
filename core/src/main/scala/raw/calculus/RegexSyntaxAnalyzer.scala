package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

import scala.util.parsing.combinator.{RegexParsers, PackratParsers}
import scala.util.parsing.input.CharSequenceReader

object RegexSyntaxAnalyzer extends RegexParsers with LazyLogging {

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

  lazy val term0: Parser[RawRegex] =
    positioned(
      ("(" ~> attrName) ~ (":" ~> term2) <~ ")" ^^ { case idn ~ r => RegexGroup(r, Some(idn))} |
      ("(" ~> term2 <~ ")" ^^ { case r => RegexGroup(r, None)}) |
      term25
  )

  lazy val term2: Parser[RegexPipe] =
    positioned(rep1sep(term225, "|") ^^ { case rs => RegexPipe(rs) })

  lazy val term225 =
    term25 |
    "" ^^^ RegexSeq(Seq())

  lazy val term25: Parser[RegexSeq] =
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

  lazy val word: Parser[RegexWord] =
    positioned("\\w" ^^^ RegexWord())

  lazy val space: Parser[RegexSpace] =
    positioned("\\s" ^^^ RegexSpace())

  lazy val digit: Parser[RegexDigit] =
    positioned("\\d" ^^^ RegexDigit())

  lazy val wildcard: Parser[RegexWildcard] =
    positioned("." ^^^ RegexWildcard())

  lazy val char: Parser[RegexChar] =
    positioned(
      "\\\\" ^^^ RegexChar('\\') |
      "\\[" ^^^ RegexChar('[') |
      "\\]" ^^^ RegexChar(']') |
      "\\(" ^^^ RegexChar('(') |
      "\\)" ^^^ RegexChar(')') |
      "\\{" ^^^ RegexChar('{') |
      "\\}" ^^^ RegexChar('}') |
      "\\*" ^^^ RegexChar('*') |
      "\\." ^^^ RegexChar('.') |
      "\\+" ^^^ RegexChar('+') |
      "\\?" ^^^ RegexChar('?') |
      "\\^" ^^^ RegexChar('^') |
      "\\$" ^^^ RegexChar('$') |
      """[^\\\[\](){}*.+?^$]""".r ^^ { case c => logger.debug("char"); RegexChar(c.toCharArray.head) })

  lazy val plus: Parser[RegexPlus] =
    positioned(term4 <~ "+" ^^ { case r => RegexPlus(r) })

  lazy val question: Parser[RegexQuestion] =
    positioned(term4 <~ "?" ^^ { case r => RegexQuestion(r) })

  lazy val star: Parser[RegexStar] =
    positioned(term4 <~ "*" ^^ { case r => RegexStar(r) })

  lazy val repeat: Parser[RegexRepeat] =
    positioned((term4 <~ "{") ~ ("\\d+".r <~ "}") ^^ { case r ~ d => RegexRepeat(r, d.toInt) })

  lazy val beginLine: Parser[RegexBeginLine] =
    positioned("^" ^^^ RegexBeginLine())

  lazy val endLine: Parser[RegexEndLine] =
    positioned("$" ^^^ RegexEndLine())

  lazy val or: Parser[RegexOr] =
    positioned(
      "[^" ~> rep1(orContent) <~ "]" ^^ { case ors => RegexOr(ors, true)} |
      "[" ~> rep1(orContent) <~ "]" ^^ { case ors => RegexOr(ors, false)})

  lazy val orContent =
    orInterval |
    orPrimitive

  lazy val orInterval: Parser[RegexOrInterval] =
    positioned(("\\w".r <~ "-") ~ "\\w".r ^^ {case a ~ b => RegexOrInterval(a.toCharArray.head, b.toCharArray.head)})

  lazy val orPrimitive: Parser[RegexOrPrimitive] =
    positioned(primitive ^^ { case p => RegexOrPrimitive(p) })

  lazy val primitive =
    word |
    space |
    digit |
    wildcard |
    primitiveChar

  lazy val primitiveChar: Parser[RegexChar] =
    positioned(
      "\\\\" ^^^ RegexChar('[') |
      "\\[" ^^^ RegexChar('[') |
      "\\]" ^^^ RegexChar(']') |
      """[^\\\[\]]""".r ^^ { case c => RegexChar(c.toCharArray.head) })

  lazy val attrName: Parser[String] =
    escapedIdent |
    ident

  lazy val escapedIdent: Parser[String] =
    """`[\w\t ]+`""".r ^^ {case s => s.drop(1).dropRight(1) }

  // TODO: Block reserved words. Inherit from common parsing trait. All idents and reservedWords should go to the common trait, incl attrName
  lazy val ident =
    """[_a-zA-Z]\w*""".r
}


object Foo extends App {
  println("Foo1")

  val reg = """(\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)""".r
  val data1 = """slppp6.intermind.net - - [01/Aug/1995:00:00:12 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635"""
  val data2 = """ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:12 -0400] "GET /history/apollo/images/apollo-logo1.gif HTTP/1.0" 200 1173"""
  val data3 = """133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /shuttle/missions/sts-69/sts-69-patch-small.gif HTTP/1.0" 200 8083"""

  data1 match {
    case reg(host, date, method, path, protocol, version, status, size) =>
      println(host, date, method, path, protocol, version, status, size)
  }
  data2 match {
    case reg(host, date, method, path, protocol, version, status, size) =>
      println(host, date, method, path, protocol, version, status, size)
  }
  data3 match {
    case reg(host, date, method, path, protocol, version, status, size) =>
      println(host, date, method, path, protocol, version, status, size)
  }

  println("go")

  //RegexSyntaxAnalyzer("""(\w[-\w\.]*)""") match {
//  RegexSyntaxAnalyzer("""(\w[\w\.]*)""") match {
  RegexSyntaxAnalyzer("""(\w+)\w+""") match {
    case Right(ast) => println(s"SUCCESS\n${RawRegexPrettyPrinter(ast)}")
    case Left(err) => println(s"ERROR\n$err")
  }

}