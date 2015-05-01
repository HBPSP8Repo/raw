package raw
package calculus

class SyntaxAnalyzerTest extends FunTest {
  def matches(q: String, expected: String): Unit = {
    matches(q, Some(expected))
  }

  // Utility function. If `expected` is empty, the parsed string must be the same as the input string `q`.
  def matches(q: String, expected: Option[String] = None): Unit = {
    val ast = parse(q)
    val actual = CalculusPrettyPrinter(ast, 200)
    logger.debug("\n\tInput: {}\n\tParsed: {}\n\tAST: {}", q, actual, ast)

    assert(actual == expected.getOrElse(q))
  }

  def parseError(q: String, expected: Option[String] = None): Unit = {
    assert(SyntaxAnalyzer(q).isLeft)
    if (expected.isDefined)
      SyntaxAnalyzer(q) match {
        case Left(err) => assert(err.contains(expected.get))
      }
  }

  def parseError(q: String, expected: String): Unit = parseError(q, Some(expected))

  test("cern events") {
    matches("for (e1 <- Events; e1.RunNumber > 100) yield set (muon := e1.muon)")
  }

  test("paper query 1") {
    matches( """for (el <- for (d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
  }

  test("paper query 2") {
    matches( """for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)""")
  }

  test("backticks") {
    matches( """for (e <- Employees) yield set (`Employee Children` := e.children)""")
  }

  test("record projections") {
    matches("""("Foo", "Bar")._1""", """(_1 := "Foo", _2 := "Bar")._1""")
    matches("""((`Employee Name` := "Ben", Age := 35).`Employee Name`, "Foo")._1""",
      """(_1 := (`Employee Name` := "Ben", Age := 35).`Employee Name`, _2 := "Foo")._1""")
  }

  test("expression blocks") {
    matches( """{ a := 1; a + 1 }""")
    matches( """for (d <- Departments) yield set { name := d.name; (deptName := name) }""")
  }

  test("patterns") {
    matches("""{ (a, b) := (1, 2); a + b }""",
      """{ (a, b) := (_1 := 1, _2 := 2); a + b }""")
    matches("""{ (a, (b, c)) := (_1 := 1, _2 := (_1 := 2, _2 := 3)); a + b + c }""")
    matches("""\(a, b) -> a + b + 1""")
    matches("""\(a, (b, c)) -> a + b + c + 1""")
    matches("""for ((a, b) <- list((1, 2), (3, 4))) yield set a + b""",
      """for ((a, b) <- list((_1 := 1, _2 := 2)) append list((_1 := 3, _2 := 4))) yield set a + b""")
  }

  /*
    Primitive Types:      bool, int, float, string,
    Collection Types:     set, bag, list,
    Record Types:         record
    Class Type: ???

    Unary operators:      -, not, to_bool, to_int, to_float, to_string
    Binary operators:     -, /, %
    Monoids Boolean:      or, and
    Monoids Number:       +, *, max

    Container operators:  union, bag_union, append, max, sum,

    Constants:            null, true, false,

    Control:              for, yield, if, then, else,
   */

  /** Parentheses and operator precedence:
    * http://en.wikipedia.org/wiki/Order_of_operations#Programming_languages
    */
  test("parentheses - sum - none") {
    matches("a + b + c", "a + b + c")
  }

  test("parentheses - sum - redundant 1") {
    matches("(a + b) + c", "a + b + c")
  }

  test("parentheses - sum - redundant 2") {
    matches("((a + b) + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 3") {
    matches("a + (b + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 4") {
    matches("(a + (b + c))", "a + b + c")
  }

  test("parentheses - sum/mult - none") {
    matches("a + b * c", "a + b * c")
  }

  ignore("parentheses - sum/mult - required") {
    matches("(a + b) * c", "(a + b) * c")
  }

  ignore("parentheses - sum/mult - some required") {
    matches("((a + b) * c)", "(a + b) * c")
  }

  test("parentheses - sum/mult - redundant") {
    matches("a + (b * c)", "a + b * c")
  }

  test("parentheses - subtraction - none") {
    matches("a - b - c", "a - b - c")
  }

  test("parentheses - subtraction - redundant") {
    matches("(a - b) - c", "a - b - c")
  }

  ignore("parentheses - subtraction - required") {
    matches("a - (b - c)", "a - (b - c)")
  }

  test("parentheses - division - none") {
    matches("a / b / c", "a / b / c")
  }

  test("parentheses - division - redundant") {
    matches("(a / b) / c", "a / b / c")
  }

  ignore("parentheses - division - required") {
    matches("a / (b / c)", "a / (b / c)")
  }

  ignore("parentheses - division - some required") {
    matches("((a / b) / (c / d))", "a / b / (c / d)")
  }

  ignore("parentheses - unary minus - #51") {
    matches("-a", "-a")
  }

  ignore("parentheses - double unary minus - #51") {
    matches("-(-a)", "a")
  }

  ignore("parentheses - unary plus - #51") {
    matches("+a", "+a")
  }

  test("parentheses - equality - redundant") {
    matches("(a = b)", "a = b")
  }

  test("parentheses - equality - none") {
    matches("a = b", "a = b")
  }

  ignore("parentheses - lt and comparison - constants") {
    matches("2 < 4 = false", "2 < 4 = false")
  }

  ignore("parentheses - lt and comparison - variables - none") {
    matches("a < b = c", "a < b = c")
  }

  test("parentheses - lt and comparison - variables - redundant") {
    matches("(a < b) = c", "a < b = c")
  }

  test("#63 (support for !=)") {
    matches("""{ a := 1 != 2; a }""", """{ a := 1 <> 2; a }""")
  }

  test("source code comments") {
    matches(
      """
        // compute the max of values
        for (v <- values) // yield set v
           yield max v
           // check if type inference assumes v is a int/float
      """, """for (v <- values) yield max v""")
  }

  test("#71 (keywords issue)") {
    matches("""{ v := nothing; v }""")
    matches("""{ v := nottrue; v }""")
    parseError("""for (v <- collection) yield unionv""", "illegal monoid")
    parseError("""for (v <- collection) yield bag_unionv""", "illegal monoid")
    parseError("""for (v <- collection) yield appendv""", "illegal monoid")
    parseError("""for (v <- collection) yield setv""", "illegal monoid")
    parseError("""for (v <- collection) yield listv""", "illegal monoid")
    parseError("""for (v <- collection) yield bagv""", "illegal monoid")
    parseError("""for (v <- booleans) yield andv""", "illegal monoid")
    parseError("""for (v <- booleans) yield andfalse""", "illegal monoid")
    parseError("""for (v <- booleans) yield orv""", "illegal monoid")
    parseError("""for (v <- booleans) yield ortrue""", "illegal monoid")
    parseError("""for (v <- numbers) yield maxv""", "illegal monoid")
    parseError("""for (v <- numbers) yield max12""", "illegal monoid")
    parseError("""for (v <- numbers) yield sumv""", "illegal monoid")
    parseError("""for (v <- numbers) yield sum123""", "illegal monoid")
    parseError("""for (v <- numbers) yield multiplyv""", "illegal monoid")
    parseError("""for (v <- numbers) yield multiply123""", "illegal monoid")
    matches("""for (r <- records; v := trueandfalse) yield set v""")
    matches("""for (r <- records; v := falseortrue) yield set v""")
    matches("""{ v := iftruethen1else2; v }""")
  }
}