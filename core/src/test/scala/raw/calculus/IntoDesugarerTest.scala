package raw
package calculus

class IntoDesugarerTest extends CalculusTest {

  val phase = "IntoDesugarer"

  test("(number: 1, 2) into (column1: number, column2: _2)") {
    check(
      """(number: 1, 2) into (column1: number, column2: _2)""",
      """{ $0 := (number: 1, _2: 2); (column1: $0.number, column2: $0._2) }""",
      TestWorlds.empty
    )
  }

  test("(x: ( (a: 1, b: 2) into (a: b, b: a) ), 3) into (column1: x, column2: _2)") {
    check(
      """(x: ( (a: 1, b: 2) into (a: b, b: a) ), 3) into (column1: x, column2: _2)""",
      """{ $1 := (x: { $0 := (a: 1, b: 2); (a: $0.b, b: $0.a) }, _2: 3); (column1: $1.x, column2: $1._2) }""",
      TestWorlds.empty
    )
  }

}
