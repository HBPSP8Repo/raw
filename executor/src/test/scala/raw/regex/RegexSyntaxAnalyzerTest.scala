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

  test("""(host:\w[-\w\./,:@*]*).*""") {
    matches("""(host:\w[-\w\./,:@*]*).*""")
  }

}
