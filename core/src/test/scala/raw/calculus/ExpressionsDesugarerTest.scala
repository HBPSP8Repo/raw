package raw
package calculus

class ExpressionsDesugarerTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Phases(t, w, lastTransform = Some("ExpressionsDesugarer"))

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    logger.debug(CalculusPrettyPrinter(t1.root, 200))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("sum bag") {
    compare(
      process(
        """sum(bag(1))"""),
        """\$0 -> for ($1 <- $0) yield sum $1(bag(1))""")
  }

  test("sum list") {
    compare(
      process(
        """sum(list(1))"""),
      """\$0 -> for ($1 <- $0) yield sum $1(list(1))""")
  }

  test("sum set") {
    compare(
      process(
        """sum(set(1))"""),
      """\$0 -> for ($1 <- $0) yield sum $1(to_bag(set(1)))""")
  }

  test("max list") {
    compare(
      process(
        """max(list(1))"""),
      """\$0 -> for ($1 <- $0) yield max $1(list(1))""")
  }

  test("max set") {
    compare(
      process(
        """max(set(1))"""),
      """\$0 -> for ($1 <- $0) yield max $1(set(1))""")
  }

  test("count bag") {
    compare(
      process(
        """count(bag(1))"""),
      """\$0 -> for ($1 <- $0) yield sum 1(bag(1))""")
  }

  test("count list") {
    compare(
      process(
        """count(list(1))"""),
      """\$0 -> for ($1 <- $0) yield sum 1(list(1))""")
  }

  test("count set") {
    compare(
      process(
        """count(set(1))"""),
      """\$0 -> for ($1 <- $0) yield sum 1(to_bag(set(1)))""")
  }

}
