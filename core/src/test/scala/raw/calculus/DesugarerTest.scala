package raw
package calculus

class DesugarerTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)

    val desugarer = new Desugarer { val tree = t; val world = w }
    desugarer.errors.foreach(err => logger.error(err.toString))
    assert(desugarer.errors.length === 0)

    CalculusPrettyPrinter(desugarer.transform.root, 200)
  }

  test("desugar ExpBlock") {
    compare(
      process(
        """for (d <- Departments) yield set { name := d.name; (deptName := name) }""", TestWorlds.departments),
      """for ($0 <- Departments) yield set (deptName := $0.name)""")
  }

  test("desugar PatternFunAbs") {
    compare(
      process(
        """\(a: int, b: int) -> a + b + 2"""),
      """\ $0 -> $0._1 + $0._2 + 2""")
  }

  test("desugar PatternGen") {
    compare(
      process(
        """for ((a, b, c, d) <- things) yield set (a, d)""", TestWorlds.things),
      """for ($0 <- things; $1 := $0._1; $2 := $0._2; $3 := $0._3; $4 := $0._4) yield set (_1 := $1, _2 := $4)""")
  }

  test("desugar PatternBind") {
    compare(
      process(
        """{ (a, b) := (1, 2); a + b }"""),
      """(_1 := 1, _2 := 2)._1 + (_1 := 1, _2 := 2)._2""")
  }

}
