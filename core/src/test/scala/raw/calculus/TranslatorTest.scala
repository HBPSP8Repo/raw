package raw
package calculus

class TranslatorTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val analyzer = new SemanticAnalyzer(t, w)
    analyzer.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
    assert(analyzer.errors.length === 0)

    val t1 = Phases(t, w, lastTransform = Some("Translator"))

    val analyzer1 = new SemanticAnalyzer(t1, w)
    analyzer1.errors.foreach(err => logger.error(ErrorsPrettyPrinter(err)))
    assert(analyzer1.errors.length === 0)

    assert(analyzer.tipe(t.root) === analyzer1.tipe(t1.root))

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("select s.name from students s") {
    compare(
      process(
        """select s.name from students s""", TestWorlds.professors_students),
        """for ($0 <- students) yield list $0.name""")
  }

  test("select s.name from students s where s.age > 10") {
    compare(
      process(
        """select s.name from students s where s.age > 10""", TestWorlds.professors_students),
      """for ($0 <- students; $0.age > 10) yield list $0.name""")
  }

  test("select s.age/10, partition from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10, partition from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- students) yield list (_1: $0.age / 10, _2: for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1)""")
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, partition as students from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- students) yield list (decade: $0.age / 10, students: for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1)""")
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- students) yield list (decade: $0.age / 10, names: for ($2 <- for ($1 <- students; $0.age / 10 = $1.age / 10) yield list $1) yield list $2.name)""")
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    compare(
      process(
        """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""", TestWorlds.professors_students),
        """for ($0 <- students) yield list
              (_1:    $0.age,
               names: for ($1 <- for ($2 <- students; $0.age = $2.age) yield list $2) yield list
                            (_1: $1.name,
                             _2: for ($3 <- for ($2 <- students; $0.age = $2.age) yield list $2; $1.name = $3.name) yield list $3))""")
  }

  test("select A from authors A") {
    compare(
      process(
        "select A from authors A", TestWorlds.publications),
        "for ($0 <- authors) yield bag $0"
      )
  }

  test("select P from publications P") {
    compare(
      process(
        "select P from publications P", TestWorlds.publications),
        "for ($0 <- publications) yield bag $0"
    )
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    compare(
      process(
        "select A.title, count(partition) as n from authors A group by A.title", TestWorlds.publications),
        """for ($0 <- authors) yield bag (_1: $0.title, n: \$1 -> for ($2 <- $1) yield sum 1(for ($3 <- authors; $0.title = $3.title) yield bag $3))"""
    )
  }

  test("select A.title, partition as people from authors A group by A.title") {
    compare(
      process(
        "select A.title, partition as people from authors A group by A.title", TestWorlds.publications),
      """for ($0 <- authors) yield bag (_1: $0.title, people: for ($3 <- authors; $0.title = $3.title) yield bag $3)"""
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    compare(
      process(
        "select A.title, (select A.year from A in partition) as years from authors A group by A.title", TestWorlds.publications),
      """for ($0 <- authors) yield bag (_1: $0.title, years: for ($3 <- for ($4 <- authors; $0.title = $4.title) yield bag $4) yield bag $3.year)"""
    )
  }

  test("distinct leads to a set") {
    compare(
      process("select distinct A.title from A in authors", TestWorlds.publications),
      "for ($0 <- authors) yield set $0.title"
      )
    }


  test("2-level group by (title, year, count)") {
    compare(
      process(
        """select G.title,
                 (select distinct year: v.year,
                         N: count(partition)
                  from v in G.people
                  group by v.year) as stats
           from (
                 select title: A.title, partition as people
                 from authors A
                 group by A.title) G""", TestWorlds.publications),
      """for ($10 <- for ($0 <- authors) yield bag (title: $0.title, people: for ($3 <- authors; $0.title = $3.title) yield bag $3))
          yield bag (_1: $10.title, stats: for ($12 <- $10.people) yield set (year: $12.year, N: \$13 -> for ($14 <- $13) yield sum 1(for ($22 <- $10.people; $12.year = $22.year) yield bag $22)))
      """
    )
  }

  test("2-level group by (title, year, count) #2") {
    compare(
      process("""
        select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title
      """, TestWorlds.publications),"""
        for ($0 <- authors) yield set (title: $0.title,stats: for ($1 <- for ($3 <- authors; $0.title = $3.title) yield bag $3) yield bag (year: $1.year, N: \$9 -> for ($4 <- $9) yield sum 1(for ($5 <- for ($3 <- authors; $0.title = $3.title) yield bag $3; $1.year = $5.year) yield bag $5)))""")
  }

  test("x in L") {
    compare(
      process("0 in select A.year from A in authors", TestWorlds.publications),
      "for ($1 <- for ($0 <- authors) yield bag $0.year) yield or $1 = 0"
    )
  }

  test("select x in L from x in something") {
    // select all students having a birthyear matching any of the ones found for professors
    compare(
      process("""select A.name from A in authors where A.title = "PhD" and A.year in select B.year from B in authors where B.title = "professor"""", TestWorlds.publications),
      """for ($30 <- authors;
              $30.title = "PhD"
              and for ($84 <- for ($37 <- authors; $37.title = "professor")
                              yield bag $37.year)
                  yield or $84 = $30.year
              )
         yield bag $30.name"""
    )
  }

  test("a in publications.authors") {
    compare(process("""select p from p in publications where "Donald Knuth" in p.authors""", TestWorlds.publications),
            """for ($13 <- publications; for ($33 <- $13.authors) yield or $33 = "Donald Knuth") yield bag $13""")
  }

}
