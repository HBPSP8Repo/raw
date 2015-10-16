package raw
package calculus

class StarDesugarerTest extends CalculusTest {

  val phase = "StarDesugarer"

  test("select * from students") {
    check(
      """select * from students""",
      """select (name: $0.name, age: $0.age) from $0 <- students""",
      TestWorlds.professors_students,
      ignoreRootTypeComparison = true)
  }

  test("select * from list(1,2,3)") {
    check(
      """select * from list(1,2,3)""",
      """select $0 from $0 <- list(1, 2, 3)""",
      TestWorlds.professors_students)
  }

  test("select * from list(1,2,3) l") {
    check(
      """select * from list(1,2,3) l""",
      """select l from l <- list(1, 2, 3)""",
      TestWorlds.professors_students)
  }

  test("select * from students, professors") {
    check(
      """select * from students, professors""",
      """select (name: $0.name, age: $0.age, name_1: $1.name, age_1: $1.age) from $0 <- students, $1 <- professors""",
      TestWorlds.professors_students)
  }

  test("select * from students s, professors") {
    check(
      """select * from students s, professors""",
      """select (name: s.name, age: s.age, name_1: $0.name, age_1: $0.age) from s <- students, $0 <- professors""",
      TestWorlds.professors_students)
  }

  test("select * from students, professors p") {
    check(
      """select * from students, professors p""",
      """select (name: $0.name, age: $0.age, name_1: p.name, age_1: p.age) from $0 <- students, p <- professors""",
      TestWorlds.professors_students)
  }

  test("select * from students s, professors p") {
    check(
      """select * from students s, professors p""",
      """select (name: s.name, age: s.age, name_1: p.name, age_1: p.age) from s <- students, p <- professors""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, * from students s group by s.age/10") {
    check(
      """select s.age/10, * from students s group by s.age/10""",
      """select (_1: s.age / 10, _2: select (name: $0.name, age: $0.age) from $0 <- students where s.age / 10 = $0.age / 10) from s <- students""",
      TestWorlds.professors_students,
      ignoreRootTypeComparison = true)
  }

  test("select *, * from students s group by s.age") {
    check(
      """select *, * from students s group by s.age""",
      """select (_1: select (name: $0.name, age: $0.age) from $0 <- students where s.age = $0.age, _2: select (name: $0.name, age: $0.age) from $0 <- students where s.age = $0.age) from s <- students""",
      TestWorlds.professors_students,
      ignoreRootTypeComparison = true)
  }

  test("select * from (select * from students)") {
    check(
      """select * from (select * from students)""",
      """select (name: $0.name, age: $0.age) from $0 <- select (name: $1.name, age: $1.age) from $1 <- students""",
      TestWorlds.professors_students,
      ignoreRootTypeComparison = true)
  }

}