package raw.calculus

import raw.World

class SimplifierTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new SemanticAnalyzer(t, w)
    assert(analyzer.errors.length === 0)
    CalculusPrettyPrinter(Simplifier(t, w).root, 200)
  }

  ignore("test1") {
    compare(
      process(TestWorlds.things, """for (t <- things, t.a + 5 + 10 * 2 > t.b + 2 - 5 * 3) yield set t.a""" ),
      """for ($0 <- things, 25 + $0.a > -13 + $0.b) yield set $0.a"""
    )
  }

}
