package raw
package calculus

class StarDesugarerTest extends CalculusTest {

  val phase = "StarDesugarer"

  test("select * from students") {
    check(
      """select * from students""",
      """select $0 from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select * from students, professors") {
    check(
      """select * from students, professors""",
      """select $0, $1 from $0 <- students, $1 <- professors""",
      TestWorlds.professors_students)
  }

}
