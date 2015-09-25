package raw
package calculus

class OptimizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new calculus.SemanticAnalyzer(t, w)
    logger.debug(calculus.CalculusPrettyPrinter(ast))
    val t_q = analyzer.tipe(t.root)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)
    logger.debug(s"t_q $t_q")

    val algebra = Optimizer(t, w)
    logger.debug(s"algebra\n${CalculusPrettyPrinter(algebra.root)}")
    val analyzer2 = new calculus.SemanticAnalyzer(algebra, w)
    analyzer2.errors.foreach(err => logger.error(err.toString))
    assert(analyzer2.errors.length === 0)
    val t_alg = analyzer2.tipe(algebra.root)
    compare(TypesPrettyPrinter(t_alg), TypesPrettyPrinter(t_q))
    algebra
  }

  def check(query: String, world: World, algebra: String) = compare(CalculusPrettyPrinter(process(world, query).root), algebra)

  test("exp block is created because of to_bag and to_set found in the tree") {
    check("select A.title from A in (select distinct A from A in authors)", TestWorlds.publications,
      """{ $32 := to_bag(to_set(reduce(bag, $16 <- authors, $16))); reduce(bag, $14 <- $32, $14.title) }""")
  }

  test("publications: select A from A in authors") {
    check("select A from A in authors", TestWorlds.publications, """reduce(bag, $0 <- authors, $0)""")
  }

  test("publications: count of authors grouped by title and then year") {
    check("select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title",
          TestWorlds.publications, """
          """)
  }

}