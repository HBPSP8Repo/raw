  package raw
  package calculus

  class NormalizerTest extends CalculusTest {

    val phase = "Normalizer"

    test("rule1 #1") {
      check(
        "for (e <- Events; e1 := e; (for (a <- Events; a1 := a; a1.RunNumber > 200) yield max a1.RunNumber) < 300; e1.RunNumber > 100) yield set (muons: e1.muons)",
        "for ($0 <- Events; for ($1 <- Events; $1.RunNumber > 200) yield max $1.RunNumber < 300; $0.RunNumber > 100) yield set (muons: $0.muons)",
        TestWorlds.cern)
    }

    test("rule1 #2") {
      check(
        "for (e <- Events; a := e.RunNumber + 1; b := a + 2) yield set b",
        "for ($0 <- Events) yield set $0.RunNumber + 1 + 2",
        TestWorlds.cern)
    }

    test("rule1 #3") {
      check(
        "for (e <- Events; a := e.RunNumber + 1; b := e.RunNumber + 2) yield set a + b",
        "for ($0 <- Events) yield set $0.RunNumber + 1 + $0.RunNumber + 2",
        TestWorlds.cern)

    }

    test("rule1 #4") {
      check(
        "(for (e <- Events; a := e) yield set a) union (for (e <- Events; a := e) yield set a)",
        "for ($0 <- Events) yield set $0 union for ($1 <- Events) yield set $1",
        TestWorlds.cern)
    }

    test("rule1 #5") {
      check(
        "for (x := for (e <- Events) yield set e; y <- x) yield set y",
        "for ($0 <- Events) yield set $0",
        TestWorlds.cern)
    }

    test("rule2 #1") {
      check(
        """for (e <- Events; e.RunNumber > ((\v -> v + 2)(40))) yield set e""",
        "for ($0 <- Events; $0.RunNumber > 40 + 2) yield set $0",
        TestWorlds.cern)
    }

    test("rule2 #2") {
      check(
        """for (e <- Events; f := (\v -> v + 2); e.RunNumber > f(40)) yield set e""",
        """for ($0 <- Events; $0.RunNumber > 40 + 2) yield set $0""",
        TestWorlds.cern)
    }

    test("rule2 #3") {
      check(
        """for (e <- Events; e1 := e; (for (a <- Events; a1 := a; f := (\v -> v + 2); a1.RunNumber > f(40)) yield max a1.RunNumber) < 300; f2 := (\v -> v + 4); e1.RunNumber > f2(100)) yield set (muons: e1.muons)""",
        """for ($0 <- Events; for ($1 <- Events; $1.RunNumber > 40 + 2) yield max $1.RunNumber < 300; $0.RunNumber > 100 + 4) yield set (muons: $0.muons)""",
        TestWorlds.cern)
    }

    test("rule2 #4") {
      check(
         """for (y <- ((\x -> x bag_union x)(for (a <- authors) yield bag a))) yield bag y""",
         """for ($0 <- authors) yield bag $0 bag_union for ($1 <- authors) yield bag $1""",
        TestWorlds.authors_publications)
    }

    test("rule3") {
      check(
        """for (e <- Events; rec := (x: e.RunNumber, y: e.lbn); rec.x > 200) yield sum 1""",
        """for ($0 <- Events; $0.RunNumber > 200) yield sum 1""",
        TestWorlds.cern)
    }

    test("rule4") {
      check(
        """for (t <- things; thing <- if t.a > t.b then t.set_a else t.set_b; thing > 10.0) yield set thing""",
        """for ($0 <- things; $0.a > $0.b; $1 <- $0.set_a; $1 > 10.0) yield set $1 union for ($2 <- things; not $2.a > $2.b; $3 <- $2.set_b; $3 > 10.0) yield set $3""",
        TestWorlds.things)
    }

    test("rule5") {
      check(
        """for (t <- list()) yield sum 1""",
        """0""",
        TestWorlds.things)
    }

    test("rule6") {
      // Rule 6 + Rule 1
      check(
        """for (t <- list(1); t > 1) yield list t""",
        """for (1 > 1) yield list 1""",
        TestWorlds.things)
    }

    test("rule7") {
      check(
        """for (t <- things; t1 <- t.set_a union t.set_b; t1 > 10.0) yield set t1""",
        """for ($0 <- things; $1 <- $0.set_a; $1 > 10.0) yield set $1 union for ($2 <- things; $3 <- $2.set_b; $3 > 10.0) yield set $3""",
        TestWorlds.things)
    }

    test("rule8") {
      check(
        """for (t <- for (t <- things; t.a > 10) yield set t; t.b > 20) yield set t""",
        """for ($0 <- things; $0.a > 10; $0.b > 20) yield set $0""",
        TestWorlds.things)
    }

    test("rule9") {
      check(
        """for (for (t <- things; t.b > 20) yield or t.a > t.b; 1 > 2) yield set true""",
        """for ($0 <- things; $0.b > 20; $0.a > $0.b; 1 > 2) yield set true""",
        TestWorlds.things)
    }

    test("rule10") {
      check(
        """for (1 > 2) yield sum (for (e <- Events) yield sum e.lbn)""",
        """for (1 > 2; $0 <- Events) yield sum $0.lbn""",
        TestWorlds.cern)
    }

    test("paper query") {
      check(
        """for (el <- for (d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name: e.name, Address: e.address)""",
        """for ($0 <- Departments; $0.name = "CSE"; $1 <- $0.instructors; $2 <- $1.teaches; $2.name = "cse5331") yield set (Name: $1.name, Address: $1.address)""",
        TestWorlds.departments)
    }

    test("join") {
      check(
        """for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)""",
        """for ($0 <- speed_limits; $1 <- radar; $0.location = $1.location; $1.speed < $0.min_speed or $1.speed > $0.max_speed) yield list (name: $1.person, location: $1.location)""",
        TestWorlds.fines)
    }

    test("desugar and normalize PatternGen") {
      check(
        """for ((a, b, c, d) <- set_of_tuples) yield set (a, d)""",
        """for ($0 <- set_of_tuples) yield set (a: $0._1, d: $0._4)""",
        TestWorlds.set_of_tuples)
    }

    test("desugar and normalize PatternBind") {
      check(
        """{ (a, b) := (1, 2); a + b }""",
        """1 + 2""")
    }

  }

