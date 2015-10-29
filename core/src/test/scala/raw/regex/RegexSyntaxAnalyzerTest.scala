package raw
package regex

class RegexSyntaxAnalyzerTest extends RawTest {

  private def parse(q: String): Regex.RegexNode = {
    RegexSyntaxAnalyzer(q) match {
      case Right(ast) => ast
      case Left(err) => fail(s"Parser error: $err")
    }
  }

  private def matches(q: String, expected: String): Unit = {
    matches(q, Some(expected))
  }

  private def matches(q: String, expected: Option[String] = None): Unit = {
    val ast = parse(q)
    val pretty = RegexPrettyPrinter(ast, 200).replaceAll("\\s+", " ").trim()
    val e = expected.getOrElse(q).replaceAll("\\s+", " ").trim()
    logger.debug("\n\tInput: {}\n\tParsed: {}\n\tAST: {}", q, pretty, ast)

    assert(pretty === e)
  }

  test("""(host:\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)""") {
    matches("""(host:\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)""")
  }

//
//
//  object Foo extends App {
//    println("Foo1")
//
//    val reg = """(\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)""".r
//    val data1 = """slppp6.intermind.net - - [01/Aug/1995:00:00:12 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635"""
//    val data2 = """ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:12 -0400] "GET /history/apollo/images/apollo-logo1.gif HTTP/1.0" 200 1173"""
//    val data3 = """133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /shuttle/missions/sts-69/sts-69-patch-small.gif HTTP/1.0" 200 8083"""
//
//    data1 match {
//      case reg(host, date, method, path, protocol, version, status, size) =>
//        println(host, date, method, path, protocol, version, status, size)
//    }
//    data2 match {
//      case reg(host, date, method, path, protocol, version, status, size) =>
//        println(host, date, method, path, protocol, version, status, size)
//    }
//    data3 match {
//      case reg(host, date, method, path, protocol, version, status, size) =>
//        println(host, date, method, path, protocol, version, status, size)
//    }
//
//    println("go")
//
//    def collectGroups(n: RawRegex): Seq[RegexGroup] = n match {
//      case RegexSeq(rs) => rs.flatMap(collectGroups)
//      case RegexPipe(rs) => rs.flatMap(collectGroups)
//      case RegexWord() => Seq()
//      case RegexSpace() => Seq()
//      case RegexDigit() => Seq()
//      case RegexWildcard() => Seq()
//      case RegexChar(c) => Seq()
//      case RegexEscapedChar(c) => Seq()
//      case RegexPlus(r) => collectGroups(r)
//      case RegexQuestion(r) => collectGroups(r)
//      case RegexStar(r) => collectGroups(r)
//      case RegexRepeat(r, _) => collectGroups(r)
//      case RegexBeginLine() => Seq()
//      case RegexEndLine() => Seq()
//      case RegexOr(ors, not) => ors.flatMap{
//        case RegexOrInterval(a, b) => Seq()
//        case RegexOrPrimitive(p) => collectGroups(p)
//      }
//      case n1 @ RegexGroup(r, _) => Seq(n1) ++ collectGroups(r)
//    }
//
//    //  RegexSyntaxAnalyzer("""(\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)""") match {
//    val x = """(host:\w[-\w\.]*)\s+-\s+-\s+\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2}\s[-+]\d{4})\]\s+"(\w+)\s+([^ ]+)\s(\w+)/(\d+.\d+)"\s+(\d+)\s+(\d+)"""
//    RegexSyntaxAnalyzer(x) match {
//      //  RegexSyntaxAnalyzer("""(\w+)\w+""") match {
//      case Right(ast) =>
//        val y = RawRegexPrettyPrinter(ast)
//        println(ast)
//        assert(x == y)
//        println(collectGroups(ast).length)
//        println(collectGroups(ast).head.r)
//        println(s"SUCCESS\n$y")
//      case Left(err) => println(s"ERROR\n$err")
//    }
//
//  }
}
