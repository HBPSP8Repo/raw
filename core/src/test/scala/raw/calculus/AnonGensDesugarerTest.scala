package raw
package calculus

class AnonGensDesugarerTest extends CalculusTest {

  val phase = "AnonGensDesugarer"

  test("for ( <- students) yield set age") {
    check(
      """for ( <- students) yield set age""",
      """for ($0 <- students) yield set $0.age""",
      TestWorlds.professors_students
    )
  }

  test("for ( <- students; age > for ( <- professors) yield max age) yield set age") {
    check(
      """for ( <- students; age > for ( <- professors) yield max age) yield set age""",
      """for ($0 <- students; $0.age > for ($1 <- professors) yield max $1.age) yield set $0.age""",
      TestWorlds.professors_students
    )
  }

  test("for ( <- students ) yield set (age, age > for ( <- professors) yield max age)") {
    check(
      """for ( <- students ) yield set (age, age > for ( <- professors) yield max age)""",
      """for ($0 <- students ) yield set (age: $0.age, _2: $0.age > for ($1 <- professors) yield max $1.age)""",
      TestWorlds.professors_students
    )
  }

}
