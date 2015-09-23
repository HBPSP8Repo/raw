package raw
package calculus

class TranslatorTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Translator(t, w)

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  // TODO: Make the testcases get the root type of both types and compare that they are equal!!!!

  test("select s.name from students s") {
    compare(
      process(
        """select s.name from students s""", TestWorlds.professors_students),
        """for ($0 <- students) yield bag $0.name"""
    )
  }

  test("select s.name from students s where s.age > 10") {
    compare(
      process(
        """select s.name from students s where s.age > 10""", TestWorlds.professors_students),
      """for ($0 <- students; $0.age > 10) yield bag $0.name"""
    )
  }


}
