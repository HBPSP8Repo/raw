package raw
package calculus

class SelectDesugarerTest extends CalculusTest {

  val phase = "SelectDesugarer"

  test("select s.name from students s") {
    check(
      """select s.name from students s""",
      """for (s$0 <- students) yield $1 s$0.name""",
      TestWorlds.professors_students)
  }

  test("select s.name from students s where s.age > 10") {
    check(
      """select s.name from students s where s.age > 10""",
      """for (s$0 <- students; s$0.age > 10) yield $1 s$0.name""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s group by s.age/10") {
    check(
      """select s.age/10, partition from students s group by s.age/10""",
      """for (s$0 <- students) yield $1 (_1: s$0.age / 10, partition: for (s$2 <- students; s$0.age / 10 = s$2.age / 10) yield $3 s$2)""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, partition as students from students s group by s.age/10""",
      """for (s$0 <- students) yield $1 (decade: s$0.age / 10, students: for (s$2 <- students; s$0.age / 10 = s$2.age / 10) yield $3 s$2)""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""",
      """for (s$0 <- students) yield $1 (decade: s$0.age / 10, names: for (s$2 <- for (s$3 <- students; s$0.age / 10 = s$3.age / 10) yield $4 s$3) yield $5 s$2.name)""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    check(
      """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""",
      """for (s$0 <- students) yield $1
            (age:    s$0.age,
             names: for (s$1 <- for (s$2 <- students; s$0.age = s$2.age) yield $4 s$2) yield $5
                          (name: s$1.name,
                           partition: for (s$6 <- for (s$7 <- students; s$0.age = s$7.age) yield $7 s$7; s$1.name = s$6.name) yield $8 s$6))""",
      TestWorlds.professors_students)
  }

  test("select A from authors A") {
    check(
      "select A from authors A",
      "for (A$0 <- authors) yield $1 A$0",
      TestWorlds.publications)
  }

  test("select P from publications P") {
    check(
      "select P from publications P",
      "for (P$0 <- publications) yield $1 P$0",
      TestWorlds.publications)
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    check(
      "select A.title, count(partition) as n from authors A group by A.title",
      """for (A$0 <- authors) yield $1 (title: A$0.title, n: count(for (A$2 <- authors; A$0.title = A$2.title) yield $3 A$2))""",
      TestWorlds.publications)
  }

  test("select A.title, partition as people from authors A group by A.title") {
    check(
      "select A.title, partition as people from authors A group by A.title",
      """for (A$0 <- authors) yield $1 (title: A$0.title, people: for (A$2 <- authors; A$0.title = A$2.title) yield $3 A$2)""",
      TestWorlds.publications
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    check(
      "select A.title, (select A.year from A in partition) as years from authors A group by A.title",
      """for (A$0 <- authors) yield $1 (title: A$0.title, years: for (A$2 <- for (A$3 <- authors; A$0.title = A$3.title) yield $4 A$3) yield $5 A$2.year)""",
      TestWorlds.publications)
  }

  test("distinct leads to a set") {
    check(
      "select distinct A.title from A in authors",
      "for (A$0 <- authors) yield set A$0.title",
      TestWorlds.publications)
  }

  test("2-level group by (title, year, count)") {
    check(
      """
       select G.title,
             (select distinct year: v.year,
                     N: count(partition)
              from v in G.people
              group by v.year) as stats
       from (
             select title: A.title, partition as people
             from authors A
             group by A.title) G
      """,
      """for (G$0 <- for (A$1 <- authors) yield $2 (title: A$1.title, people: for (A$3 <- authors; A$1.title = A$3.title) yield $4 A$3))
         yield $5 (title: G$0.title, stats: for (v$6 <- G$0.people) yield set (year: v$6.year, N: count(for (v$7 <- G$0.people; v$6.year = v$7.year) yield $8 v$7)))
      """,
      TestWorlds.publications)
  }

  test("2-level group by (title, year, count) #2") {
    check(
      """select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title """,
      """
         for (A$0 <- authors) yield set
           (title: A$0.title, stats: for (A$1 <- for (A$2 <- authors; A$0.title = A$2.title) yield $3 A$2) yield $4 (year: A$1.year, N: count(for (A$4 <- for (A$5 <- authors; A$0.title = A$5.title) yield $7 A$5; A$1.year = A$4.year) yield $5 A$4)))
      """,
      TestWorlds.publications)
  }

  test("x in L") {
    check(
      "0 in select A.year from A in authors",
      "0 in for (A$0 <- authors) yield $1 A$0.year",
      TestWorlds.publications)
  }

  test("select x in L from x in something") {
    // select all students having a birthyear matching any of the ones found for professors
    check(
      """select A.name from A in authors where A.title = "PhD" and A.year in select B.year from B in authors where B.title = "professor"""",
      """for (A$0 <- authors; A$0.title = "PhD" and A$0.year in for (B$1 <- authors; B$1.title = "professor") yield $2 B$1.year) yield $3 A$0.name""",
      TestWorlds.publications)
  }

  test("a in publications.authors") {
    check(
      """select p from p in publications where "Donald Knuth" in p.authors""",
      """for (p$0 <- publications; "Donald Knuth" in p$0.authors) yield $1 p$0""",
      TestWorlds.publications)
  }

}
