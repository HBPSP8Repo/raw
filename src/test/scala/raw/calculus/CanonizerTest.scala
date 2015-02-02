package raw.calculus

import raw.World

class CanonizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new SemanticAnalyzer(t, w)
    assert(analyzer.errors.length === 0)
    CalculusPrettyPrinter.pretty(Canonizer(t, w).root, 200)
  }

  test("paper_query") {
   compare(
      process(TestWorlds.departments, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)"""),
      """for ($0 <- Departments, $1 <- $0.instructors, $2 <- $1.teaches, $0.name = "CSE", $2.name = "cse5331") yield set (Name := $1.name, Address := $1.address)"""
    )
  }

  test("top_level_merge") {
    compare(
      process(TestWorlds.things, "for (x <- things union things) yield set x"),
      """for ($0 <- things) yield set $0 union for ($1 <- things) yield set $1"""
    )
  }

}
