package raw.calculus

import raw.World

class CanonizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val canonizer = new Canonizer { val world = w }
    assert(canonizer.errors(ast).length === 0)
    CanonicalCalculusPrettyPrinter.pretty(canonizer.canonize(ast), 200)
  }

  test("paper_query") {
   compare(
      process(TestWorlds.departments, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)"""),
      """for ($var0 <- Departments, $var1 <- $var0.instructors, $var2 <- $var1.teaches, $var0.name = "CSE", $var2.name = "cse5331") yield set (Name := $var1.name, Address := $var1.address)"""
    )
  }

}
