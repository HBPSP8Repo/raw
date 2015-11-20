package raw
package calculus

class BlocksDesugarerTest extends PhaseTest {

  val phase = "BlocksDesugarer"

  test("ExpBlock") {
    check(
      """for (d <- Departments) yield set { name := d.name; (deptName: name) }""",
      """for (d$0 <- Departments) yield set (deptName: d$0.name)""",
      TestWorlds.departments)
  }

  ignore("#122 PatternFunAbs") {
    check(
      """\(a, b) -> a + b + 2""",
      """\$0 -> $0._1 + $0._2 + 2""")
  }

  test("PatternGen") {
    check(
      """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""",
      """for ($0 <- set_of_tuples; a$1 := $0._1; b$2 := $0._2; c$3 := $0._3; d$4 := $0._4) yield set (a: a$1, d: d$4)""",
      TestWorlds.set_of_tuples)
  }

  test("PatternBind") {
    check(
      """{ (a, b) := (1, 2); a + b }""",
      """(_1: 1, _2: 2)._1 + (_1: 1, _2: 2)._2""")
  }

  test("nested ExpBlocks") {
    check(
      """{ a := 1; { a := 2; a } + a }""",
      """2 + 1""")
  }

}
