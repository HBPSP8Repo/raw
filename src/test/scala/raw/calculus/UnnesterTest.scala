package raw.calculus

import org.scalatest.FunSuite

class UnnesterTest extends FunSuite {

  def process(w: World, q: String) = {
    val c = Driver.parse(q)
    assert(w.errors(c).length === 0)
    w.unnest(c)
  }

  test("paper_query") {
    import raw.algebra.AlgebraPrettyPrinter

    // TODO: Implement actual tests

    println(AlgebraPrettyPrinter.pretty(process(TestWorlds.employees,
      """
        for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)
         """)))

    println(AlgebraPrettyPrinter.pretty(process(TestWorlds.departments,
    """for (el <- for ( d <- Departments, d.name="CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")))

    assert(false)
  }

}
