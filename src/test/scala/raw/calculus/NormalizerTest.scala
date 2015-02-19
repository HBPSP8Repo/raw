package raw
package calculus

class NormalizerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new SemanticAnalyzer(t, w)
    assert(analyzer.errors.length === 0)
    CalculusPrettyPrinter.pretty(Normalizer(t, w).root, 200)
  }

  test("rule1") {
    compare(
      process(TestWorlds.cern, "for (e <- Events, e1 := e, (for (a <- Events, a1 := a, a1.RunNumber > 200) yield max a1.RunNumber) < 300, e1.RunNumber > 100) yield set (muons := e1.muons)"),
      "for ($0 <- Events, for ($1 <- Events, $1.RunNumber > 200) yield max $1.RunNumber < 300, $0.RunNumber > 100) yield set (muons := $0.muons)")

    compare(
      process(TestWorlds.cern, "for (e <- Events, a := e.RunNumber + 1, b := a + 2) yield set b"),
      "for ($0 <- Events) yield set $0.RunNumber + 1 + 2"
    )

    compare(
      process(TestWorlds.cern, "for (e <- Events, a := e.RunNumber + 1, b := e.RunNumber + 2) yield set a + b"),
      "for ($0 <- Events) yield set $0.RunNumber + 1 + $0.RunNumber + 2"
    )

    compare(
      process(TestWorlds.cern, "(for (e <- Events, a := e) yield set a) union (for (e <- Events, a := e) yield set a)"),
      "for ($0 <- Events) yield set $0 union for ($1 <- Events) yield set $1"
    )

    compare(
      process(TestWorlds.cern, "for (x := for (e <- Events) yield set e, y <- x) yield set y"),
      "for ($0 <- Events) yield set $0"
    )
  }

  test("rule2") {
    compare(
      process(TestWorlds.cern, """for (e <- Events, e.RunNumber > (\v: int -> v + 2)(40)) yield set e"""),
      "for ($0 <- Events, $0.RunNumber > 40 + 2) yield set $0")

    compare(
      process(TestWorlds.cern, """for (e <- Events, f := (\v: int -> v + 2), e.RunNumber > f(40)) yield set e"""),
      "for ($0 <- Events, $0.RunNumber > 40 + 2) yield set $0")

    compare(
      process(TestWorlds.cern, """for (e <- Events, e1 := e, (for (a <- Events, a1 := a, f := (\v: int -> v + 2), a1.RunNumber > f(40)) yield max a1.RunNumber) < 300, f2 := (\v: int -> v + 4), e1.RunNumber > f2(100)) yield set (muons := e1.muons)"""),
      "for ($0 <- Events, for ($1 <- Events, $1.RunNumber > 40 + 2) yield max $1.RunNumber < 300, $0.RunNumber > 100 + 4) yield set (muons := $0.muons)")
  }

  test("rule3") {
    compare(
      process(TestWorlds.cern, """for (e <- Events, rec := (x := e.RunNumber, y := e.lbn), rec.x > 200) yield sum 1"""),
      """for ($0 <- Events, $0.RunNumber > 200) yield sum 1""")
  }

  test("rule4") {
    compare(
      process(TestWorlds.things, """for (t <- things, thing <- if t.a > t.b then t.set_a else t.set_b, thing > 10.0) yield set thing"""),
      """for ($0 <- things, $0.a > $0.b, $1 <- $0.set_a, $1 > 10.0) yield set $1 union for ($2 <- things, not $2.a > $2.b, $3 <- $2.set_b, $3 > 10.0) yield set $3""")
  }

  test("rule5") {
    compare(
      process(TestWorlds.things, """for (t <- list()) yield sum 1"""),
      """0""")
  }

  test("rule6") {
    // Rule 6 + Rule 1
    compare(
      process(TestWorlds.things, """for (t <- list(1), t > 1) yield list t"""),
      """for (1 > 1) yield list 1""")
  }

  test("rule7") {
    compare(
      process(TestWorlds.things, """for (t <- things, t1 <- t.set_a union t.set_b, t1 > 10.0) yield set t1"""),
      """for ($0 <- things, $1 <- $0.set_a, $1 > 10.0) yield set $1 union for ($2 <- things, $3 <- $2.set_b, $3 > 10.0) yield set $3""")
  }

  test("rule8") {
    compare(
      process(TestWorlds.things, """for (t <- for (t <- things, t.a > 10) yield set t, t.b > 20) yield set t"""),
      """for ($0 <- things, $0.a > 10, $0.b > 20) yield set $0""")
  }

  test("rule9") {
    compare(
      process(TestWorlds.things, """for (for (t <- things, t.b > 20) yield or t.a > t.b, 1 > 2) yield set true"""),
      """for ($0 <- things, $0.b > 20, $0.a > $0.b, 1 > 2) yield set true""")
  }

  test("rule10") {
    compare(
      process(TestWorlds.cern, """for (1 > 2) yield sum (for (e <- Events) yield sum e.lbn)"""),
      """for (1 > 2, $0 <- Events) yield sum $0.lbn""")
  }

  test("paper_query") {
    compare(
      process(TestWorlds.departments, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)"""),
      """for ($0 <- Departments, $0.name = "CSE", $1 <- $0.instructors, $2 <- $1.teaches, $2.name = "cse5331") yield set (Name := $1.name, Address := $1.address)""")
  }

}
