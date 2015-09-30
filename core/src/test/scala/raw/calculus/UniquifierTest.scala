package raw
package calculus

class UniquifierTest extends CalculusTest {

  val phase = "Uniquifier1"

  test("different entities are unique entities") {
    check(
      """for (a <- things; b <- things) yield set (a: a, b: b)""",
      """for ($0 <- things; $1 <- things) yield set (a: $0, b: $1)""",
      TestWorlds.things)
  }

  test("different entities are unique entities across nested scopes") {
    check(
      """for (a <- things) yield set (a: a, b: for (a <- things) yield set a)""",
      """for ($0 <- things) yield set (a: $0, b: for ($1 <- things) yield set $1)""",
      TestWorlds.things)
  }

  test("""\(a, b) -> a + b + 1""") {
    check("""\(a, b) -> a + b + 1""", """\($0, $1) -> $0 + $1 + 1""")
  }

  test("""\(a, b) -> a + b + 2""") {
    check("""\(a, b) -> a + b + 2""", """\($0, $1) -> $0 + $1 + 2""")
  }

}
