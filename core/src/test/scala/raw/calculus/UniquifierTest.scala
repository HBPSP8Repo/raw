package raw
package calculus

class UniquifierTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Desugarer(t)
    val t2 = Uniquifier(t1, w)

    val analyzer = new SemanticAnalyzer(t2, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t2.root, 200)
  }

  test("different entities are unique entities") {
    compare(
      process(
        """for (a <- things; b <- things) yield set (a := a, b := b)""", TestWorlds.things),
        """for ($0 <- things; $1 <- things) yield set (a := $0, b := $1)""")
  }

  test("different entities are unique entities across nested scopes") {
    compare(
      process(
        """for (a <- things) yield set (a := a, b := for (a <- things) yield set a)""", TestWorlds.things),
        """for ($0 <- things) yield set (a := $0, b := for ($1 <- things) yield set $1)""")
  }

  test("FunAbs with pattern") {
    compare(process("""\(a, b) -> a + b"""), """\$0 -> { $1 := $0._1; $2 := $0._2; $1 + $2 }""")
  }

}
