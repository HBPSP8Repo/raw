package raw.calculus

import org.scalatest.FunSuite

class NormalizerTest extends FunSuite {

  import Calculus.Exp

  def process(w: World, q: String) = {
    val c = Driver.parse(q)
    assert(w.errors(c).length === 0)
    w.normalize(c)
  }

  test("rule1") {
    assert(
      process(TestWorlds.cern, "for (e <- Events, e1 := e, (for (a <- Events, a1 := a, a1.RunNumber > 200) yield max a1.RunNumber) < 300, e1.RunNumber > 100) yield set (muons := e1.muons)").toString()
        ===
        "for (e <- Events, for (a <- Events, a.RunNumber > 200) yield max a.RunNumber < 300, e.RunNumber > 100) yield set (muons := e.muons)")
  }

  test("rule2") {
    // Rule 2 only 
    assert(
      process(TestWorlds.cern, """for (e <- Events, e.RunNumber > (\v: int => v + 2)(40)) yield set e""").toString()
        ===
        """for (e <- Events, e.RunNumber > 40 + 2) yield set e""")

    // Rule 1 + Rule 2
    assert(
      process(TestWorlds.cern, """for (e <- Events, f := (\v: int => v + 2), e.RunNumber > f(40)) yield set e""").toString()
        ===
        "for (e <- Events, e.RunNumber > 40 + 2) yield set e")

    // Rule 1 + Rule 2
    assert(
      process(TestWorlds.cern, """for (e <- Events, e1 := e, (for (a <- Events, a1 := a, f := (\v: int => v + 2), a1.RunNumber > f(40)) yield max a1.RunNumber) < 300, f2 := (\v: int => v + 4), e1.RunNumber > f2(100)) yield set (muons := e1.muons)""").toString()
        ===
        "for (e <- Events, for (a <- Events, a.RunNumber > 40 + 2) yield max a.RunNumber < 300, e.RunNumber > 100 + 4) yield set (muons := e.muons)")
  }

  test("rule3") {
    assert(
      process(TestWorlds.cern, """for (e <- Events, rec := (x := e.RunNumber, y := e.lbn), rec.x > 200) yield sum 1""").toString()
        ===
        """for (e <- Events, e.RunNumber > 200) yield sum 1""")
  }

  test("rule4") {
    assert(
      process(TestWorlds.things, """for (t <- things, thing <- if t.a > t.b then t.set_a else t.set_b, thing > 10.0) yield set thing""").toString()
        ===
        """for (t <- things, t.a > t.b, thing <- t.set_a, thing > 10.0) yield set thing union for (t <- things, not t.a > t.b, thing <- t.set_b, thing > 10.0) yield set thing""")
  }

  test("rule5") {
    assert(
      process(TestWorlds.things, """for (t <- []) yield sum 1""").toString()
        ===
        """0""")
  }

  test("rule6") {
    // Rule 6 + Rule 1
    assert(
      process(TestWorlds.things, """for (t <- [1], t > 1) yield list t""").toString()
        ===
        """for (1 > 1) yield list 1""")
  }

  test("rule7") {
    assert(
      process(TestWorlds.things, """for (t <- things, t1 <- t.set_a union t.set_b, t1 > 10.0) yield set t1""").toString()
        ===
        """for (t <- things, t1 <- t.set_a, t1 > 10.0) yield set t1 union for (t <- things, t1 <- t.set_b, t1 > 10.0) yield set t1""")
  }

  test("rule8") {
    assert(
      process(TestWorlds.things, """for (t <- for (t <- things, t.a > 10) yield set t, t.b > 20) yield set t""").toString()
        ===
        """for (t <- things, t.a > 10, t.b > 20) yield set t""")
  }

  test("rule9") {
    assert(
      process(TestWorlds.things, """for (for (t <- things, t.b > 20) yield or t.a > t.b, 1 > 2) yield set true""").toString()
        ===
        """for (t <- things, t.b > 20, t.a > t.b, 1 > 2) yield set true""")
  }

  test("rule10") {
    assert(
      process(TestWorlds.cern, """for (1 > 2) yield sum (for (e <- Events) yield sum e.lbn)""").toString()
        ===
        """for (1 > 2, e <- Events) yield sum e.lbn""")
  }

  test("paper_query") {
    val world = TestWorlds.departments
    val nc = process(world, """for (el <- for (d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (Name := e.name, Address := e.address)""")

    assert(
      nc.toString()
        ===
        """for (d <- Departments, d.name = "CSE", e <- d.instructors, c <- e.teaches, c.name = "cse5331") yield set (Name := e.name, Address := e.address)"""
        )
    assert(
      world.tipe(nc.asInstanceOf[Exp]).toString()
        ===
        "set(record(Name = string, Address = record(street = string, zipcode = string)))")
  }

}
