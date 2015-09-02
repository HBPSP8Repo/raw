package raw
package calculus

class NormalizerTest extends FunTest {

  def process(q: String, w: World = TestWorlds.empty) = {
    val t = new Calculus.Calculus(parse(q))

    val t1 = Normalizer(t, w)

    val analyzer = new SemanticAnalyzer(t1, w)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)

    CalculusPrettyPrinter(t1.root, 200)
  }

  test("rule1") {
    compare(
      process(
        "for (e <- Events; e1 := e; (for (a <- Events; a1 := a; a1.RunNumber > 200) yield max a1.RunNumber) < 300; e1.RunNumber > 100) yield set (muons := e1.muons)", TestWorlds.cern),
        "for ($0 <- Events; for ($1 <- Events; $1.RunNumber > 200) yield max $1.RunNumber < 300; $0.RunNumber > 100) yield set (muons := $0.muons)")

    compare(
      process(
        "for (e <- Events; a := e.RunNumber + 1; b := a + 2) yield set b", TestWorlds.cern),
        "for ($0 <- Events) yield set $0.RunNumber + 1 + 2"
    )

    compare(
      process(
        "for (e <- Events; a := e.RunNumber + 1; b := e.RunNumber + 2) yield set a + b", TestWorlds.cern),
        "for ($0 <- Events) yield set $0.RunNumber + 1 + $0.RunNumber + 2"
    )

    compare(
      process(
        "(for (e <- Events; a := e) yield set a) union (for (e <- Events; a := e) yield set a)", TestWorlds.cern),
        "for ($0 <- Events) yield set $0 union for ($1 <- Events) yield set $1"
    )

    compare(
      process(
        "for (x := for (e <- Events) yield set e; y <- x) yield set y", TestWorlds.cern),
        "for ($0 <- Events) yield set $0"
    )
  }

  test("rule2") {
    compare(
      process(
        """for (e <- Events; e.RunNumber > ((\v -> v + 2)(40))) yield set e""", TestWorlds.cern),
        "for ($0 <- Events; $0.RunNumber > 40 + 2) yield set $0")

    compare(
      process(
        """for (e <- Events; f := (\v -> v + 2); e.RunNumber > f(40)) yield set e""", TestWorlds.cern),
        "for ($0 <- Events; $0.RunNumber > 40 + 2) yield set $0")

    compare(
      process(
        """for (e <- Events; e1 := e; (for (a <- Events; a1 := a; f := (\v -> v + 2); a1.RunNumber > f(40)) yield max a1.RunNumber) < 300; f2 := (\v -> v + 4); e1.RunNumber > f2(100)) yield set (muons := e1.muons)""", TestWorlds.cern),
        "for ($0 <- Events; for ($1 <- Events; $1.RunNumber > 40 + 2) yield max $1.RunNumber < 300; $0.RunNumber > 100 + 4) yield set (muons := $0.muons)")
  }

  test("rule3") {
    compare(
      process(
        """for (e <- Events; rec := (x := e.RunNumber, y := e.lbn); rec.x > 200) yield sum 1""", TestWorlds.cern),
        """for ($0 <- Events; $0.RunNumber > 200) yield sum 1""")
  }

  test("rule4") {
    compare(
      process(
        """for (t <- things; thing <- if t.a > t.b then t.set_a else t.set_b; thing > 10.0) yield set thing""", TestWorlds.things),
        """for ($0 <- things; $0.a > $0.b; $1 <- $0.set_a; $1 > 10.0) yield set $1 union for ($2 <- things; not $2.a > $2.b; $3 <- $2.set_b; $3 > 10.0) yield set $3""")
  }

  test("rule5") {
    compare(
      process(
        """for (t <- list()) yield sum 1""", TestWorlds.things),
        """0""")
  }

  test("rule6") {
    // Rule 6 + Rule 1
    compare(
      process(
        """for (t <- list(1); t > 1) yield list t""", TestWorlds.things),
        """for (1 > 1) yield list 1""")
  }

  test("rule7") {
    compare(
      process(
        """for (t <- things; t1 <- t.set_a union t.set_b; t1 > 10.0) yield set t1""", TestWorlds.things),
        """for ($0 <- things; $1 <- $0.set_a; $1 > 10.0) yield set $1 union for ($2 <- things; $3 <- $2.set_b; $3 > 10.0) yield set $3""")
  }

  test("rule8") {
    compare(
      process(
        """for (t <- for (t <- things; t.a > 10) yield set t; t.b > 20) yield set t""", TestWorlds.things),
      """for ($0 <- things; $0.a > 10; $0.b > 20) yield set $0""")
  }

  test("rule9") {
    compare(
      process(
        """for (for (t <- things; t.b > 20) yield or t.a > t.b; 1 > 2) yield set true""", TestWorlds.things),
        """for ($0 <- things; $0.b > 20; $0.a > $0.b; 1 > 2) yield set true""")
  }

  test("rule10") {
    compare(
      process(
        """for (1 > 2) yield sum (for (e <- Events) yield sum e.lbn)""", TestWorlds.cern),
        """for (1 > 2; $0 <- Events) yield sum $0.lbn""")
  }

  test("paper query") {
    compare(
      process(
        """for (el <- for (d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)""", TestWorlds.departments),
        """for ($0 <- Departments; $0.name = "CSE"; $1 <- $0.instructors; $2 <- $1.teaches; $2.name = "cse5331") yield set (Name := $1.name, Address := $1.address)""")
  }

  test("join") {
    compare(
      process(
        """for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)""", TestWorlds.fines),
        """for ($0 <- speed_limits; $1 <- radar; $0.location = $1.location; $1.speed < $0.min_speed or $1.speed > $0.max_speed) yield list (name := $1.person, location := $1.location)"""
    )
  }

  test("desugar and normalize PatternGen") {
    compare(
      process(
        """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""", TestWorlds.set_of_tuples),
      """for ($0 <- set_of_tuples) yield set (_1 := $0._1, _2 := $0._4)""")
  }

  test("desugar and normalize PatternBind") {
    compare(
      process(
        """{ (a, b) := (1, 2); a + b }"""),
      """1 + 2""")
  }

}

