package raw
package calculus

class ExpressionsDesugarerTest extends CalculusTest {

  val phase = "ExpressionsDesugarer"

  test("sum bag") {
    check(
      """sum(bag(1))""",
      """\$0 -> for ($1 <- $0) yield sum $1(bag(1))""")
  }

  test("sum list") {
    check(
      """sum(list(1))""",
      """\$0 -> for ($1 <- $0) yield sum $1(list(1))""")
  }

  test("sum set") {
    check(
      """sum(set(1))""",
      """\$0 -> for ($1 <- $0) yield sum $1(to_bag(set(1)))""")
  }

  test("max list") {
    check(
      """max(list(1))""",
      """\$0 -> for ($1 <- $0) yield max $1(list(1))""")
  }

  test("max set") {
    check(
      """max(set(1))""",
      """\$0 -> for ($1 <- $0) yield max $1(set(1))""")
  }

  test("count bag") {
    check(
      """count(bag(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(bag(1))""")
  }

  test("count list") {
    check(
      """count(list(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(list(1))""")
  }

  test("count set") {
    check(
      """count(set(1))""",
      """\$0 -> for ($1 <- $0) yield sum 1(to_bag(set(1)))""")
  }

  test("list(1,2,3)") {
    check("list(1,2,3)", ???)
  }

}
