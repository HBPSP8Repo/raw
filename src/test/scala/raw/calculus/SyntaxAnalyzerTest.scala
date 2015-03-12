package raw.calculus

class SyntaxAnalyzerTest extends FunTest {

  def isOK(q: String): Unit = {
    isOK(q, q)
  }

  def isOK(q: String, expected: String): Unit = {
    info(s"$q")
    val ast = parse(q)
    info(s"$ast")
    val actual = CalculusPrettyPrinter(ast, 200)
    info(actual)
    assert(actual === expected)
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
    isOK("a + b + c", "a + b + c")
  }

  test("parentheses - sum - redundant 1") {
    isOK("(a + b) + c", "a + b + c")
  }

  test("parentheses - sum - redundant 2") {
    isOK("((a + b) + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 3") {
    isOK("a + (b + c)", "a + b + c")
  }

  test("parentheses - sum - redundant 4") {
    isOK("(a + (b + c))", "a + b + c")
  }

  test("parentheses - sum/mult - none") {
    isOK("a + b * c", "a + b * c")
  }

  ignore("parentheses - sum/mult - required") {
    isOK("(a + b) * c", "(a + b) * c")
  }

  ignore("parentheses - sum/mult - some required") {
    isOK("((a + b) * c)", "(a + b) * c")
  }

  test("parentheses - sum/mult - redundant") {
    isOK("a + (b * c)", "a + b * c")
  }

  test("parentheses - subtraction - none") {
    isOK("a - b - c", "a - b - c")
  }

  test("parentheses - subtraction - redundant") {
    isOK("(a - b) - c", "a - b - c")
  }

  ignore("parentheses - subtraction - required") {
    isOK("a - (b - c)", "a - (b - c)")
  }

  test("parentheses - division - none") {
    isOK("a / b / c", "a / b / c")
  }

  test("parentheses - division - redundant") {
    isOK("(a / b) / c", "a / b / c")
  }

  ignore("parentheses - division - required") {
    isOK("a / (b / c)", "a / (b / c)")
  }

  ignore("parentheses - division - some required") {
    isOK("((a / b) / (c / d))", "a / b / (c / d)")
  }

  ignore("parentheses - unary minus - #51") {
    isOK("-a", "-a")
  }

  ignore("parentheses - double unary minus - #51") {
    isOK("-(-a)", "a")
  }

  ignore("parentheses - unary plus - #51") {
    isOK("+a", "+a")
  }

  test("parentheses - equality - redundant") {
    isOK("(a = b)", "a = b")
  }

  test("parentheses - equality - none") {
    isOK("a = b", "a = b")
  }

  ignore("parentheses - lt and comparison - constants") {
    isOK("2 < 4 = false", "2 < 4 = false")
  }

  ignore("parentheses - lt and comparison - variables - none") {
    isOK("a < b = c", "a < b = c")
  }

  test("parentheses - lt and comparison - variables - redundant") {
    isOK("(a < b) = c", "a < b = c")
  }

  test("cern_events") {
    isOK("for (e1 <- Events, e1.RunNumber > 100) yield set (muon := e1.muon)")
  }

  test("paper_query_1") {
    isOK( """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
  }

  test("paper_query_2") {
    isOK( """for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)""")
  }
}
