package raw
package calculus

class SimplifierTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)

    val simplifier = new Simplifier { val tree = t; val world = w }
    assert(simplifier.errors.length === 0)
    CalculusPrettyPrinter(simplifier.transform.root, 200)
  }

  ignore("test1") {
    compare(
      process("""for (t <- things, t.a + 5 + 10 * 2 > t.b + 2 - 5 * 3) yield set t.a""", TestWorlds.things),
      """for ($0 <- things, 25 + $0.a > -13 + $0.b) yield set $0.a"""
    )
  }

  test("join") {
    compare(
      process(
        """for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)""", TestWorlds.fines),
        """for ($0 <- speed_limits; $1 <- radar; $0.location = $1.location; $1.speed < $0.min_speed or $1.speed > $0.max_speed) yield list (name := $1.person, location := $1.location)"""
    )
  }


}
