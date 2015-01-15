package raw.calculus

class CanonizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = w.canonize(parse(w, q))
    CanonicalCalculusPrettyPrinter.pretty(ast, 200)
  }

  /** Compare results of actual query result with expected query result, but first normalizes variable names.
    * Since variable names are auto-generated, the function normalizes all uses of $varN to a uniform integer.
    * e.g.
    *   `$var14 + $var15 - 2 * $var14` is equivalent to `$var0 + $var1 - 2 * $var0`
    */
  def compare(actual: String, expected: String) = {
    def norm(in: Iterator[String]) = {
      var m = scala.collection.mutable.Map[String, Int]()
      var c = 0
      for (v <- in)
        if (m.getOrElseUpdate(v, c) == c)
          c += 1
      m
    }
    val r = """\$var\d""".r
    assert(norm(r.findAllIn(actual)) === norm(r.findAllIn(expected)))
  }


  test("paper_query") {
   compare(
      process(TestWorlds.departments, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)"""),
      """for ($var0 <- Departments, $var1 <- $var0.instructors, $var2 <- $var1.teaches, $var0.name = "CSE", $var2.name = "cse5331") yield set (Name := $var1.name, Address := $var1.address)"""
    )
  }

}
