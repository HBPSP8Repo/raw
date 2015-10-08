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

  test("select * from students, p in professors") {
    check(
      """select * from students, p in professors""",
      """select ($0: $0,p: p) from $0 <- students, p <- professors""",
      TestWorlds.professors_students)
  }

}
