package raw.calculus

import org.scalatest.FunSuite

class CanonizerTest extends FunSuite {

  def process(w: World, q: String) = {
    val c = Driver.parse(q)
    assert(w.errors(c).length === 0)
    w.canonize(c)
  }

  test("paper_query") {
    assert(
      process(TestWorlds.departments, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)""").toString()
        ===
        """for (var1 <- Departments, var2 <- var1.instructors, var3 <- var2.teaches, var1.name = "CSE", var3.name = "cse5331") yield set (Name := var2.name, Address := var2.address)"""
    )
  }

}
