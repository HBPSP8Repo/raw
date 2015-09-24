package raw
package calculus

class TranslatorTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val analyzer = new SemanticAnalyzer(t, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    val t1 = Translator(t, w)
    val pt = CalculusPrettyPrinter(t1.root, 200)
    logger.debug(s"Result: $pt")

    val analyzer1 = new SemanticAnalyzer(t1, w)
    analyzer1.errors.foreach(err => logger.error(err.toString))
    assert(analyzer1.errors.length === 0)

    assert(analyzer.tipe(t.root) === analyzer1.tipe(t1.root))

    pt
  }

  // TODO: Make the testcases get the root type of both types and compare that they are equal!!!!

  test("select s.name from students s") {
    compare(
      process(
        """select s.name from students s""", TestWorlds.professors_students),
        """for ($0 <- students) yield bag $0.name""")
  }

  test("select s.name from students s where s.age > 10") {
    compare(
      process(
        """select s.name from students s where s.age > 10""", TestWorlds.professors_students),
      """for ($0 <- students; $0.age > 10) yield bag $0.name""")
  }

  test("select s.age/10, partition from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10, partition from students s group by s.age/10""", TestWorlds.professors_students),
        """for ($0 <- students) yield bag (_1: $0.age / 10, _2: for ($1 <- students; $0.age / 10 = $1.age / 10) yield bag $1)""")
  }

  test("select s.age/10 as decade, partition as students from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, partition as students from students s group by s.age/10""", TestWorlds.professors_students),
      """for ($0 <- students) yield bag (decade: $0.age / 10, students: for ($1 <- students; $0.age / 10 = $1.age / 10) yield bag $1)""")
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    compare(
      process(
        """select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10""", TestWorlds.professors_students),
      """for ($0 <- students) yield bag (decade: $0.age / 10, names: for ($2 <- for ($1 <- students; $0.age / 10 = $1.age / 10) yield bag $1) yield bag $2.name)""")
  }

  test("sss") {
    compare(
      process(
        "select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age", TestWorlds.professors_students),
      """for ($0 <- students) yield bag (decade: $0.age / 10, names: for ($2 <- for ($1 <- students; $0.age / 10 = $1.age / 10) yield bag $1) yield bag $2.name)""")
  }

}
