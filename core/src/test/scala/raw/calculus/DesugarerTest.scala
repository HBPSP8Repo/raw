package raw
package calculus

class DesugarerTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Desugarer(t, w)

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    logger.debug(CalculusPrettyPrinter(t1.root, 200))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("ExpBlock") {
    compare(
      process(
        """for (d <- Departments) yield set { name := d.name; (deptName: name) }""", TestWorlds.departments),
        """for ($0 <- Departments) yield set (deptName: $0.name)""")
  }

  test("PatternFunAbs") {
    compare(
      process(
        """\(a, b) -> a + b + 2"""),
        """\$0 -> $0._1 + $0._2 + 2""")
  }

  test("PatternGen") {
    compare(
      process(
        """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""", TestWorlds.set_of_tuples),
        """for ($0 <- set_of_tuples; $1 := $0._1; $2 := $0._2; $3 := $0._3; $4 := $0._4) yield set (_1: $1, _2: $4)""")
  }

  test("PatternBind") {
    compare(
      process(
        """{ (a, b) := (1, 2); a + b }"""),
        """(_1: 1, _2: 2)._1 + (_1: 1, _2: 2)._2""")
  }

  test("nested ExpBlocks") {
    compare(
      process(
        """{ a := 1; { a := 2; a } + a }"""),
        """2 + 1""")
  }

  test("sum") {
    compare(
      process(
        """sum(list(1))"""),
         """\$0 -> for ($1 <- to_bag($0)) yield sum $1(list(1))""")
  }

  test("max") {
    compare(
      process(
        """max(list(1))"""),
      """\$0 -> for ($1 <- $0) yield max $1(list(1))""")
  }


  test("count") {
    compare(
      process(
        """count(list(1))"""),
      """\$0 -> for ($1 <- to_bag($0)) yield sum 1(list(1))""")
  }

}
