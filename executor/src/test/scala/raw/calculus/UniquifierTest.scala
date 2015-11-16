package raw
package calculus

class UniquifierTest extends PhaseTest {

  val phase = "Uniquifier1"

  test("different entities are unique entities") {
    check(
      """for (a <- things; b <- things) yield set (a: a, b: b)""",
      """for (a$0 <- things; b$0 <- things) yield set (a: a$0, b: b$0)""",
      TestWorlds.things)
  }

  test("different entities are unique entities across nested scopes") {
    check(
      """for (a <- things) yield set (a: a, b: for (a <- things) yield set a)""",
      """for (a$0 <- things) yield set (a: a$0, b: for (a$1 <- things) yield set a$1)""",
      TestWorlds.things)
  }

  test("""\(a, b) -> a + b""") {
    check("""\(a, b) -> a + b""", """\a$0, b$0 -> a$0 + b$0""")
  }

  test("""\(a, b) -> a + b + 1""") {
    check("""\(a, b) -> a + b + 1""", """\a$0, b$0 -> a$0 + b$0 + 1""")
  }

}
