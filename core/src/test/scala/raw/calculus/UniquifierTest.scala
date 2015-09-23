package raw
package calculus

class UniquifierTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Uniquifier(t, w)

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("different entities are unique entities") {
    compare(
      process(
        """for (a <- things; b <- things) yield set (a: a, b: b)""", TestWorlds.things),
        """for ($0 <- things; $1 <- things) yield set (a: $0, b: $1)""")
  }

  test("different entities are unique entities across nested scopes") {
    compare(
      process(
        """for (a <- things) yield set (a: a, b: for (a <- things) yield set a)""", TestWorlds.things),
        """for ($0 <- things) yield set (a: $0, b: for ($1 <- things) yield set $1)""")
  }

  test("""\(a, b) -> a + b + 1""") {
    compare(process("""\(a, b) -> a + b + 1"""), """\($0, $1) -> $0 + $1 + 1""")
  }

  test("""\(a, b) -> a + b + 2""") {
    compare(process("""\(a, b) -> a + b + 2"""), """\($0, $1) -> $0 + $1 + 2""")
  }

}
