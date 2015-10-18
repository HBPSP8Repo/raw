package raw
package calculus

class GroupByDesugarerTest extends CalculusTest {

  val phase = "GroupByDesugarer"

  test("select s.age/10, * from students s group by s.age/10") {
    check(
      """select s.age/10, * from students s group by s.age/10""",
      """select (_1: s$0.age / 10, _2: select * from s$1 <- students where s$0.age / 10 = s$1.age / 10) from s$0 <- students""",
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
      """select (_1: s$0.age / 10, partition: select s$1 from s$1 <- students where s$0.age / 10 = s$1.age / 10) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s, professors group by s.age/10") {
    check(
      """select s.age/10, partition from students s, professors group by s.age/10""",
      """select (_1: s$0.age / 10, partition: select (s: s$1, _2: $2) from s$1 <- students, $2 <- professors where s$0.age / 10 = s$1.age / 10) from s$0 <- students, $3 <- professors""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s, professors p group by s.age/10") {
    check(
      """select s.age/10, partition from students s, professors p group by s.age/10""",
      """select (_1: s$0.age / 10, partition: select (s: s$1, p: p$2) from s$1 <- students, p$2 <- professors where s$0.age / 10 = s$1.age / 10) from s$0 <- students, p$0 <- professors""",
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
      """select (decade: s$0.age / 10, students: select s$1 from s$1 <- students where s$0.age / 10 = s$1.age / 10) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""",
      """select (decade: s$0.age / 10, names: select s$1.name from s$1 <- select s$2 from s$2 <- students where s$0.age / 10 = s$2.age / 10) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    check(
      """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""",
      """select (age: s$0.age,
                 names: select (name: s$1.name,
                                partition: select s$2 from s$2 <- select s$3 from s$3 <- students where s$0.age = s$3.age where s$1.name = s$2.name)
                        from s$1 <- select s$4 from s$4 <- students where s$0.age = s$4.age)
         from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select p.name, partition from partition p where p.age > 30 group by p.name) as names from students s group by s.age") {
    check(
      """select s.age, (select p.name, partition from partition p where p.age > 30 group by p.name) as names from students s group by s.age""",
      """select (age: s$0.age,
                 names: select (name: p$0.name,
                                partition: select p$1 from p$1 <- select s$2 from s$2 <- students where s$0.age = s$2.age where p$1.age > 30 and p$0.name = p$1.name)
                        from p$0 <- select s$3 from s$3 <- students where s$0.age = s$3.age where p$0.age > 30)
         from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    check(
      """select A.title, count(partition) as n from authors A group by A.title""",
      """select (title: A$0.title, n: count(select A$1 from A$1 <- authors where A$0.title = A$1.title)) from A$0 <- authors""",
      TestWorlds.publications)
  }

  test("select A.title, partition as people from authors A group by A.title") {
    check(
      """select A.title, partition as people from authors A group by A.title""",
      """select (title: A$0.title, people: select A$1 from A$1 <- authors where A$0.title = A$1.title) from A$0 <- authors""",
      TestWorlds.publications
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    check(
      """select A.title, (select A.year from A in partition) as years from authors A group by A.title""",
      """ select (title: A$0.title, years: select A$1.year from A$1 <- select A$2 from A$2 <- authors where A$0.title = A$2.title) from A$0 <- authors""",
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
      """select (_1: select * from s$1 <- students where s$0.age = s$1.age, partition: select s$2 from s$2 <- students where s$0.age = s$2.age) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select partition, * from students s group by s.age") {
    check(
      """select partition, * from students s group by s.age""",
      """select (partition: select s$1 from s$1 <- students where s$0.age = s$1.age, _2: select * from s$2 <- students where s$0.age = s$2.age) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select *, * from students s group by s.age") {
    check(
      """select *, * from students s group by s.age""",
      """select (_1: select * from s$1 <- students where s$0.age = s$1.age, _2: select * from s$2 <- students where s$0.age = s$2.age) from s$0 <- students""",
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
      """select (_1: count(select * from s$1 <- students where s$0.age = s$1.age), _2: select * from s$2 <- students where s$0.age = s$2.age, partition: select s$3 from s$3 <- students where s$0.age = s$3.age) from s$0 <- students""",
      TestWorlds.professors_students)
  }

  test("select *, partition from students s, professors group by s.age") {
    check(
      """select *, partition from students s, professors group by s.age""",
      """select (_1: select * from s$1 <- students, $1 <- professors where s$0.age = s$1.age, partition: select (s: s$2, _2: $4) from s$2 <- students, $4 <- professors where s$0.age = s$2.age) from s$0 <- students, $2 <- professors""",
      TestWorlds.professors_students)
  }

  test("select partition, * from students s, professors group by s.age") {
    check(
      """select partition, * from students s, professors group by s.age""",
      """select (partition: select (s: s$1, _2: $1) from s$1 <- students, $1 <- professors where s$0.age = s$1.age, _2: select * from s$2 <- students, $4 <- professors where s$0.age = s$2.age) from s$0 <- students, $2 <- professors""",
      TestWorlds.professors_students)
  }

}
