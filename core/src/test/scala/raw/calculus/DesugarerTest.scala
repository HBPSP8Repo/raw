package raw
package calculus

class DesugarerTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Desugarer(t)

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("desugar PatternFunAbs") {
    compare(
      process(
        """\(a: int, b: int) -> a + b + 2"""),
      """\ $0 -> { a : int := $0._1; b : int := $0._2; a + b + 2 }""")
  }

  test("desugar PatternGen") {
    compare(
      process(
        """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""", TestWorlds.set_of_tuples),
        """for ($0 <- set_of_tuples; a := $0._1; b := $0._2; c := $0._3; d := $0._4) yield set (_1 := a, _2 := d)""")
  }

  test("desugar PatternBind") {
    compare(
      process(
        """{ (a, b) := (1, 2); a + b }"""),
        """{ a := (_1 := 1, _2 := 2)._1; b := (_1 := 1, _2 := 2)._2; a + b }""")
  }

}
