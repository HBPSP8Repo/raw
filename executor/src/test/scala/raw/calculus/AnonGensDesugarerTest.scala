package raw.calculus

import raw.TestWorlds

class AnonGensDesugarerTest extends PhaseTest {

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

  ignore("for ( <- students) yield set *") {
    check(
      """for ( <- students) yield set *""",
      """for ($0 <- students) yield set *""",
      TestWorlds.professors_students
    )
  }

  test("select * from students") {
    check(
      """select * from students""",
      """select * from $0 <- students""",
      TestWorlds.professors_students
    )
  }

  test("select * from students, professors") {
    check(
      """select * from students, professors""",
      """select * from $0 <- students, $1 <- professors""",
      TestWorlds.professors_students)
  }

  test("select * from students, p in professors") {
    check(
      """select * from students, p in professors""",
      """select * from $0 <- students, p <- professors""",
      TestWorlds.professors_students)
  }

  test("select * from list(1,2,3) l") {
    check(
      """select * from list(1,2,3) l""",
      """select * from l <- list(1, 2, 3)""",
      TestWorlds.professors_students)
  }

  test("lookup attributes") {
    check(
      """select year from (select * from authors)""",
      """select $0.year from $0 <- select * from $1 <- authors""",
      TestWorlds.publications)
  }

  test("lookup attributes #2") {
    check(
      """select year, title from (select * from authors)""",
      """select (year: $0.year, title: $0.title) from $0 <- select * from $1 <- authors""",
      TestWorlds.publications)
  }
}
