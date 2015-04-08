package raw
package calculus

class DesugarExpBlocksTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Desugarer(t)
    val t2 = Uniquifier(t1, w)
    val t3 = DesugarExpBlocks(t2)

    logger.debug(s"t1 is ${t3.root}")
    val analyzer = new SemanticAnalyzer(t3, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t3.root, 200)
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
        """\$0 -> $0._1 + $0._2 + 2""")
  }

  test("desugar PatternGen") {
    compare(
      process(
        """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""", TestWorlds.set_of_tuples),
        """for ($0 <- set_of_tuples; $1 := $0._1; $2 := $0._2; $3 := $0._3; $4 := $0._4) yield set (_1 := $1, _2 := $4)""")
  }

  test("desugar PatternBind") {
    compare(
      process(
        """{ (a, b) := (1, 2); a + b }"""),
        """(_1 := 1, _2 := 2)._1 + (_1 := 1, _2 := 2)._2""")
  }

}
