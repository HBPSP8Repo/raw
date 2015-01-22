package raw.calculus

class SyntaxAnalyzerTest extends FunTest {

  def isOK(q: String) = assert(CalculusPrettyPrinter.pretty(parse(q), 200) === q)
  
  test("cern_events") {
    isOK("for (e1 <- Events, e1.RunNumber > 100) yield set (muon := e1.muon)")
  }
  
  test("paper_query_1") {
    isOK("""for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
  }
  
  test("paper_query_2") {
    isOK("""for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)""")
  }
}
