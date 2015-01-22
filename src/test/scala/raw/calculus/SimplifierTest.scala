package raw.calculus

import raw.World

class SimplifierTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val simplifier = new Simplifier { val world = w }
    assert(simplifier.errors(ast).length === 0)
    CanonicalCalculusPrettyPrinter.pretty(simplifier.simplify(ast), 200)
  }

  test("test1") {
    compare(
      process(TestWorlds.things, """for (t <- things, t.a + 5 + 10 * 2 > t.b + 2 - 5 * 3) yield set t.a""" ),
      """for ($var0 <- things, 25 + $var0.a > -13 + $var0.b) yield set $var0.a"""
    )
  }

  // TODO: Add tests that randomly generate expressions and compare their results to the Scala equivalent.

}
