package raw
package calculus

class OptimizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new calculus.SemanticAnalyzer(t, w)
    logger.debug(calculus.CalculusPrettyPrinter(ast))
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)
    val algebra = Optimizer(t, w)
    logger.debug(s"algebra\n${CalculusPrettyPrinter(algebra.root)}")
//    logger.debug(s"algebra\n${(algebra.root)}")
    val analyzer2 = new calculus.SemanticAnalyzer(algebra, w)
    analyzer2.errors.foreach(err => logger.error(err.toString))
    assert(analyzer2.errors.length === 0)
    compare(TypesPrettyPrinter(analyzer2.tipe(algebra.root)), TypesPrettyPrinter(analyzer.tipe(t.root)))
    algebra
  }

  def check(query: String, world: World, algebra: String) = compare(CalculusPrettyPrinter(process(world, query).root), algebra)

  test("publications: select A from A in authors") {
    check("select A from A in authors", TestWorlds.publications, """reduce(bag, $0 <- authors, $0)""")
  }

  test("publications: select distinct A from A in authors") {
    check("select distinct A from A in authors", TestWorlds.publications, """to_set(reduce(bag, $0 <- authors, $0))""")
  }

  test("sss") {
    check("select A.title from A in (select distinct A from A in authors)", TestWorlds.publications, """""")
  }

}