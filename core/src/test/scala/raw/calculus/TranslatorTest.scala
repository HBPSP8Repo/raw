package raw
package calculus

class TranslatorTest extends CalculusTest {

  val phase = "Translator"

  test("select s.name from students s") {
    check(
      """select s.name from students s""",
      """for ($0 <- students) yield list $0.name""",
      TestWorlds.professors_students)
  }

  test("select s.name from students s where s.age > 10") {
    check(
      """select s.name from students s where s.age > 10""",
      """for ($0 <- students; $0.age > 10) yield list $0.name""",
      TestWorlds.professors_students)
  }

  test("select s.age/10, partition from students s group by s.age/10") {
    // TODO: The issue is that the SemanticAnalyzer has removed some uses of UserType.
    // TODO: This is convenient because later on I do pattern match based on e.g. ColllectionType, instead of having
    // TODO: to always unwrap potentially recursive UserTypes. On the other hand, as the test code shows, we do leave
    // TODO: some user types in place, so we won't be able to infer that, e.g. the thing is a RecordType.
    // TODO: So I think we need a more coherent strategy, which is likely to report the UserType but then add another
    // TODO: type function, which is used by everyone else, that walks the user type when it is seen. Of course
    // TODO: having this handle recursive behaviour isn't easy.. humm.. Most likely just leave a single UserType
    // TODO: to walk it once?
    check(
      """select s.age/10, partition from students s group by s.age/10""",
      """for ($0 <- students) yield list (_1: $0.age / 10, _2: for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1)""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, partition as students from students s group by s.age/10""",
      """for ($0 <- students) yield list (decade: $0.age / 10, students: for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1)""",
      TestWorlds.professors_students)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    check(
      """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""",
      """for ($0 <- students) yield list (decade: $0.age / 10, names: for ($2 <- for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1) yield list $2.name)""",
      TestWorlds.professors_students)
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    check(
      """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""",
      """for ($0 <- students) yield list
            (_1:    $0.age,
             names: for ($1 <- for ($2 <- students; $0.age = $2.age) yield list $2) yield list
                          (_1: $1.name,
                           _2: for ($3 <- for ($2 <- students; $0.age = $2.age) yield list $2; $1.name = $3.name) yield list $3))""",
      TestWorlds.professors_students)
  }

  test("select A from authors A") {
    check(
      "select A from authors A",
      "for ($0 <- authors) yield bag $0",
      TestWorlds.publications)
  }

  test("select P from publications P") {
    check(
      "select P from publications P",
      "for ($0 <- publications) yield bag $0",
      TestWorlds.publications)
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    check(
      "select A.title, count(partition) as n from authors A group by A.title",
      """for ($0 <- authors) yield bag (_1: $0.title, n: \$1 -> for ($2 <- $1) yield sum 1(for ($3 <- authors; $0.title = $3.title) yield bag $3))""",
      TestWorlds.publications)
  }

  test("select A.title, partition as people from authors A group by A.title") {
    check(
      "select A.title, partition as people from authors A group by A.title",
      """for ($0 <- authors) yield bag (_1: $0.title, people: for ($3 <- authors; $0.title = $3.title) yield bag $3)""",
      TestWorlds.publications
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    check(
      "select A.title, (select A.year from A in partition) as years from authors A group by A.title",
      """for ($0 <- authors) yield bag (_1: $0.title, years: for ($3 <- for ($4 <- authors; $0.title = $4.title) yield bag $4) yield bag $3.year)""",
      TestWorlds.publications)
  }

  test("distinct leads to a set") {
    check(
      "select distinct A.title from A in authors",
      "for ($0 <- authors) yield set $0.title",
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
      """for ($10 <- for ($0 <- authors) yield bag (title: $0.title, people: for ($3 <- authors; $0.title = $3.title) yield bag $3))
          yield bag (_1: $10.title, stats: for ($12 <- $10.people) yield set (year: $12.year, N: \$13 -> for ($14 <- $13) yield sum 1(for ($22 <- $10.people; $12.year = $22.year) yield bag $22)))
      """,
      TestWorlds.publications)
  }

  test("2-level group by (title, year, count) #2") {
    check(
      """select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title """,
      """
      for ($0 <- authors) yield set (title: $0.title,stats: for ($1 <- for ($3 <- authors; $0.title = $3.title) yield bag $3) yield bag (year: $1.year, N: \$9 -> for ($4 <- $9) yield sum 1(for ($5 <- for ($3 <- authors; $0.title = $3.title) yield bag $3; $1.year = $5.year) yield bag $5)))
      """,
      TestWorlds.publications)
  }

  test("x in L") {
    check(
      "0 in select A.year from A in authors",
      "for ($1 <- for ($0 <- authors) yield bag $0.year) yield or $1 = 0",
      TestWorlds.publications)
  }

  test("select x in L from x in something") {
    // select all students having a birthyear matching any of the ones found for professors
    check(
      """select A.name from A in authors where A.title = "PhD" and A.year in select B.year from B in authors where B.title = "professor"""",
      """
       for ($30 <- authors;
            $30.title = "PhD"
            and for ($84 <- for ($37 <- authors; $37.title = "professor")
                            yield bag $37.year)
                yield or $84 = $30.year
            )
       yield bag $30.name
      """,
      TestWorlds.publications)
  }

  test("a in publications.authors") {
    check(
      """select p from p in publications where "Donald Knuth" in p.authors""",
      """for ($13 <- publications; for ($33 <- $13.authors) yield or $33 = "Donald Knuth") yield bag $13""",
      TestWorlds.publications)
  }

}
