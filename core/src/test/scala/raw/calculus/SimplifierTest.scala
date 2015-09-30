package raw
package calculus

class SimplifierTest extends CalculusTest {

  val phase = "Simplifier1"

  ignore("fold constants") {
    check(
      """for (t <- things; t.a + 5 + 10 * 2 > t.b + 2 - 5 * 3) yield set t.a""",
      """for ($0 <- things; 25 + $0.a > -13 + $0.b) yield set $0.a""",
      TestWorlds.things)
  }

  test("join") {
    check(
      """for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)""",
      """for ($0 <- speed_limits; $1 <- radar; $0.location = $1.location; $1.speed < $0.min_speed or $1.speed > $0.max_speed) yield list (name: $1.person, location: $1.location)""",
      TestWorlds.fines)
  }

  test("if true") {
    check("if (true or false) then 1 else 2", "1")
  }

  test("if false") {
    check("if (true and false) then 1 else 2", "2")
  }
}
