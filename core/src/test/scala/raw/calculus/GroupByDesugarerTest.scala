package raw
package calculus

class GroupByDesugarerTest extends CalculusTest {

  val phase = "GroupByDesugarer"

  test("select s.age/10, partition from students s group by s.age/10") {
      check(
      """select s.age/10, partition from students s group by s.age/10""",
      """select (_1: $0.age / 10, partition: select $1 from $1 <- students where $0.age / 10 = $1.age / 10) from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, partition as students from students s group by s.age/10""",
      """select (decade: $0.age / 10, students: select $1 from $1 <- students where $0.age / 10 = $1.age / 10) from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""",
      """select (decade: $0.age / 10,names: select $1.name from $1 <- select $2 from $2 <- students where $0.age / 10 = $2.age / 10) from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select age, (select name, partition from partition where age > 30 group by name) as names from students group by age") {
    check(
      """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""",
      """select (age: $0.age,
                 names: select (name: $1.name,
                                partition: select $2 from $2 <- select $3 from $3 <- students where $0.age = $3.age where $1.name = $2.name)
                        from $1 <- select $3 from $3 <- students where $0.age = $3.age)
         from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    check(
      """select A.title, count(partition) as n from authors A group by A.title""",
      """select (title: $0.title, n: count(select $1 from $1 <- authors where $0.title = $1.title)) from $0 <- authors""".stripMargin,
      TestWorlds.publications)
  }

  test("select A.title, partition as people from authors A group by A.title") {
    check(
      """select A.title, partition as people from authors A group by A.title""",
      """select (title: $0.title, people: select $1 from $1 <- authors where $0.title = $1.title) from $0 <- authors""",
      TestWorlds.publications
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    check(
      """select A.title, (select A.year from A in partition) as years from authors A group by A.title""",
      """ select (title: $0.title, years: select $1.year from $1 <- select $2 from $2 <- authors where $0.title = $2.title) from $0 <- authors""",
      TestWorlds.publications)
  }

}
