package raw
package calculus

class GroupByDesugarerTest extends CalculusTest {

  val phase = "GroupByDesugarer"

  test("select s.age/10, * from students s group by s.age/10") {
    check(
      """select s.age/10, * from students s group by s.age/10""",
      """select (_1: s.age / 10, _2: select * from $0 <- students where s.age / 10 = $0.age / 10) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select age/10, * from students group by age/10") {
    check(
      """select age/10, * from students group by age/10""",
      """select (_1: $0.age / 10, _2: select * from $1 <- students where $0.age / 10 = $1.age / 10) from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s group by s.age/10") {
      check(
      """select s.age/10, partition from students s group by s.age/10""",
      """select (_1: s.age / 10, partition: select $0 from $0 <- students where s.age / 10 = $0.age / 10) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s, professors group by s.age/10") {
    check(
      """select s.age/10, partition from students s, professors group by s.age/10""",
      """select (_1: s.age / 10, partition: select (s: $0, _2: $1) from $0 <- students, $1 <- professors where s.age / 10 = $0.age / 10) from s <- students, $2 <- professors""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s, professors p group by s.age/10") {
    check(
      """select s.age/10, partition from students s, professors p group by s.age/10""",
      """select (_1: s.age / 10, partition: select (s: $0, p: $1) from $0 <- students, $1 <- professors where s.age / 10 = $0.age / 10) from s <- students, p <- professors""",
      TestWorlds.professors_students)
  }

  test("select age/10, partition from students group by age/10") {
    check(
      """select age/10, partition from students group by age/10""",
      """select (_1: $0.age / 10, partition: select $1 from $1 <- students where $0.age / 10 = $1.age / 10) from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, partition as students from students s group by s.age/10""",
      """select (decade: s.age / 10, students: select $0 from $0 <- students where s.age / 10 = $0.age / 10) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""",
      """select (decade: s.age / 10, names: select s.name from s <- select $2 from $2 <- students where s.age / 10 = $2.age / 10) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    check(
      """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""",
      """select (age: s.age,
                 names: select (name: s.name,
                                partition: select $2 from $2 <- select $3 from $3 <- students where s.age = $3.age where s.name = $2.name)
                        from s <- select $3 from $3 <- students where s.age = $3.age)
         from s <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select p.name, partition from partition p where p.age > 30 group by p.name) as names from students s group by s.age") {
    check(
      """select s.age, (select p.name, partition from partition p where p.age > 30 group by p.name) as names from students s group by s.age""",
      """select (age: s.age,
                 names: select (name: p.name,
                                partition: select $0 from $0 <- select $1 from $1 <- students where s.age = $1.age where $0.age > 30 and p.name = $0.name)
                        from p <- select $1 from $1 <- students where s.age = $1.age where p.age > 30)
         from s <- students""",
      TestWorlds.professors_students)
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    check(
      """select A.title, count(partition) as n from authors A group by A.title""",
      """select (title: A.title, n: count(select $0 from $0 <- authors where A.title = $0.title)) from A <- authors""".stripMargin,
      TestWorlds.publications)
  }

  test("select A.title, partition as people from authors A group by A.title") {
    check(
      """select A.title, partition as people from authors A group by A.title""",
      """select (title: A.title, people: select $0 from $0 <- authors where A.title = $0.title) from A <- authors""",
      TestWorlds.publications
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    check(
      """select A.title, (select A.year from A in partition) as years from authors A group by A.title""",
      """ select (title: A.title, years: select A.year from A <- select $2 from $2 <- authors where A.title = $2.title) from A <- authors""",
      TestWorlds.publications)
  }

  test("select * from students") {
    check(
      """select * from students""",
      """select * from $0 <- students""",
      TestWorlds.professors_students)
  }

  test("select *, partition from students s group by s.age") {
    check(
      """select *, partition from students s group by s.age""",
      """select (_1: select * from $0 <- students where s.age = $0.age, partition: select $1 from $1 <- students where s.age = $1.age) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select partition, * from students s group by s.age") {
    check(
      """select partition, * from students s group by s.age""",
      """select (partition: select $0 from $0 <- students where s.age = $0.age, _2: select * from $1 <- students where s.age = $1.age) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select *, * from students s group by s.age") {
    check(
      """select *, * from students s group by s.age""",
      """select (_1: select * from $0 <- students where s.age = $0.age, _2: select * from $0 <- students where s.age = $0.age) from s <- students""",
      TestWorlds.professors_students,
      ignoreRootTypeComparison = true)
  }

  ignore("select partition, partition from students s group by s.age") {
    // TODO: Need to automatically rename the 2nd occurrence of partition to partition_1; seems easy but then we
    //       may be conflicting with a user field named 'partition_1', so we must better thing this through a bit more.
    check(
      """select partition, partition from students s group by s.age""",
      """select (partition: select $0 from $0 <- students where s.age = $0.age, partition_1: select $0 from $0 <- students where s.age = $0.age) from s <- students""",
      TestWorlds.professors_students)
      //ignoreRootTypeComparison = true) ???
  }

  test("select count(*), *, partition from students s group by s.age") {
    check(
      """select count(*), *, partition from students s group by s.age""",
      """select (_1: count(select * from $0 <- students where s.age = $0.age), _2: select * from $0 <- students where s.age = $0.age, partition: select $1 from $1 <- students where s.age = $1.age) from s <- students""",
      TestWorlds.professors_students)
  }

  test("select *, partition from students s, professors group by s.age") {
    check(
      """select *, partition from students s, professors group by s.age""",
      """select (_1: select * from $0 <- students, $1 <- professors where s.age = $0.age, partition: select (s: $2, _2: $3) from $2 <- students, $3 <- professors where s.age = $2.age) from s <- students, $4 <- professors""",
      TestWorlds.professors_students)
  }

  test("select partition, * from students s, professors group by s.age") {
    check(
      """select partition, * from students s, professors group by s.age""",
      """select (partition: select (s: $0, _2: $1) from $0 <- students, $1 <- professors where s.age = $0.age, _2: select * from $2 <- students, $3 <- professors where s.age = $2.age) from s <- students, $4 <- professors""",
      TestWorlds.professors_students)
  }

}
