package raw
package calculus

class TranslatorTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val analyzer = new SemanticAnalyzer(t, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    val t1 = Phases(t, w, lastTransform = Some("Translator"))

    val analyzer1 = new SemanticAnalyzer(t1, w)
    analyzer1.errors.foreach(err => logger.error(err.toString))
    assert(analyzer1.errors.length === 0)

    assert(analyzer.tipe(t.root) === analyzer1.tipe(t1.root))

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("select s.name from students s") {
    compare(
      process(
        """select s.name from students s""", TestWorlds.professors_students),
        """for ($0 <- to_bag(students)) yield bag $0.name""")
  }

  test("select s.name from students s where s.age > 10") {
    compare(
      process(
        """select s.name from students s where s.age > 10""", TestWorlds.professors_students),
      """for ($0 <- to_bag(students); $0.age > 10) yield bag $0.name""")
  }

  test("select s.age/10, partition from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10, partition from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- to_bag(students)) yield bag (_1: $0.age / 10, _2: for ($1 <- to_bag(students); $0.age / 10 = $1.age / 10) yield bag $1)""")
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, partition as students from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- to_bag(students)) yield bag (decade: $0.age / 10, students: for ($1 <- to_bag(students); $0.age / 10 = $1.age / 10) yield bag $1)""")
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- to_bag(students)) yield bag (decade: $0.age / 10, names: for ($2 <- to_bag(for ($1 <- to_bag(students); $0.age / 10 = $1.age / 10) yield bag $1)) yield bag $2.name)""")
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    compare(
      process(
        """select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age""", TestWorlds.professors_students),
        """for ($0 <- to_bag(students)) yield bag
              (_1:    $0.age,
               names: for ($1 <- to_bag(for ($2 <- to_bag(students); $0.age = $2.age) yield bag $2)) yield bag
                            (_1: $1.name,
                             _2: for ($3 <- to_bag(for ($2 <- to_bag(students); $0.age = $2.age) yield bag $2); $1.name = $3.name) yield bag $3))""")
  }

  test("select A from authors A") {
    compare(
      process(
        "select A from authors A", TestWorlds.publications),
        "for ($0 <- to_bag(authors)) yield bag $0"
      )
  }

  test("select P from publications P") {
    compare(
      process(
        "select P from publications P", TestWorlds.publications),
        "for ($0 <- to_bag(publications)) yield bag $0"
    )
  }

  test("select A.title, count(partition) as n from authors A group by A.title") {
    compare(
      process(
        "select A.title, count(partition) as n from authors A group by A.title", TestWorlds.publications),
        """for ($0 <- to_bag(authors)) yield bag (_1: $0.title, n: \$1 -> for ($2 <- $1) yield sum 1(for ($3 <- to_bag(authors); $0.title = $3.title) yield bag $3))"""
    )
  }

  test("select A.title, partition as people from authors A group by A.title") {
    compare(
      process(
        "select A.title, partition as people from authors A group by A.title", TestWorlds.publications),
      """for ($0 <- to_bag(authors)) yield bag (_1: $0.title, people: for ($3 <- to_bag(authors); $0.title = $3.title) yield bag $3)"""
    )
  }

  test("select A.title, (select A.year from A in partition) as years from authors A group by A.title") {
    compare(
      process(
        "select A.title, (select A.year from A in partition) as years from authors A group by A.title", TestWorlds.publications),
      """for ($0 <- to_bag(authors)) yield bag (_1: $0.title, years: for ($3 <- to_bag(for ($4 <- to_bag(authors); $0.title = $4.title) yield bag $4)) yield bag $3.year)"""
    )
  }

  test("distinct leads to a set") {
    compare(
      process("select distinct A.title from A in authors", TestWorlds.publications),
      "to_set(for ($0 <- to_bag(authors)) yield bag $0.title)"
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
      """for ($10 <- to_bag(for ($0 <- to_bag(authors)) yield bag (title: $0.title, people: for ($3 <- to_bag(authors); $0.title = $3.title) yield bag $3)))
          yield bag (_1: $10.title, stats: to_set(for ($12 <- to_bag($10.people)) yield bag (year: $12.year, N: \$13 -> for ($14 <- $13) yield sum 1(for ($22 <- to_bag($10.people); $12.year = $22.year) yield bag $22))))
      """
    )
  }

  test("2-level group by (title, year, count) #2") {
    compare(
      process("""
        select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title
      """, TestWorlds.publications),"""
        to_set(for ($0 <- to_bag(authors)) yield bag (title: $0.title,stats: for ($1 <- to_bag(for ($3 <- to_bag(authors); $0.title = $3.title) yield bag $3)) yield bag (year: $1.year, N: \$9 -> for ($4 <- $9) yield sum 1(for ($5 <- to_bag(for ($3 <- to_bag(authors); $0.title = $3.title) yield bag $3); $1.year = $5.year) yield bag $5))))""")
  }

}
