package raw
package calculus

class SemanticAnalyzerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val analyzer = new SemanticAnalyzer(new Calculus.Calculus(ast), w)
    if (analyzer.errors.nonEmpty) {
      logger.error(s"Semantic analyzer errors: ${analyzer.errors}")
    }
    assert(analyzer.errors.length === 0)

    analyzer.debugTreeTypes

    analyzer.tipe(ast)
  }

  test("cern events") {
    assert(
      process(TestWorlds.cern, "for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)")
        ===
        SetType(
          RecordType(List(
            AttrType("muon", RecordType(List(
              AttrType("pt", FloatType()),
              AttrType("eta", FloatType()))))))))
  }

  test("paper query 1") {
    assert(
      process(TestWorlds.departments, """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
        ===
        SetType(
          RecordType(List(
            AttrType("name", StringType()),
            AttrType("address", RecordType(List(
              AttrType("street", StringType()),
              AttrType("zipcode", StringType()))))))))
  }

  test("paper query 2") {
    assert(
      process(TestWorlds.employees, "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)")
        ===
        SetType(
          RecordType(List(
            AttrType("E",
              RecordType(List(
                AttrType("children", ListType(
                  RecordType(List(
                    AttrType("age", IntType()))))),
                AttrType("manager", RecordType(List(
                  AttrType("name", StringType()),
                  AttrType("children", ListType(
                    RecordType(List(
                      AttrType("age", IntType()))))))))))),
            AttrType("M", IntType())))))
  }

  test("simple inference") {
    assert(
      process(new World(Map("unknown" -> MemoryLocation(SetType(AnyType()), Nil))), """for (l <- unknown) yield set (l + 2)""")
      ===
      SetType(IntType())
    )
  }

  test("inference with function") {
    assert(
      process(new World(Map("unknown" -> MemoryLocation(SetType(AnyType()), Nil))), """for (l <- unknown; f := (\v -> v + 2)) yield set f(l)""")
      ===
      SetType(IntType())
    )
}

  test("infer across bind") {
    assert(
      process(new World(Map("unknown" -> MemoryLocation(SetType(AnyType()), Nil))), """for (j <- unknown; v := j) yield set (v + 0)""")
      ===
      SetType(IntType())
    )
  }

  test("expression block type") {
    assert(
      process(TestWorlds.departments, """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""")
        ===
        SetType(
          RecordType(List(
            AttrType("deptName", StringType())))))
  }

  def checkTopType(query: String, t: Type, issue: String = "") = {
    val run = (d: String) => if (issue != "") ignore(d + s" ($issue)") _ else test(d) _
    run(query) {
      val world = new World(
        Map("unknown" -> MemoryLocation(SetType(AnyType()), Nil),
          "integers" -> MemoryLocation(SetType(IntType()), Nil),
          "floats" -> MemoryLocation(SetType(FloatType()), Nil),
          "booleans" -> MemoryLocation(SetType(BoolType()), Nil),
          "strings" -> MemoryLocation(SetType(StringType()), Nil),
          "records" -> MemoryLocation(SetType(RecordType(scala.collection.immutable.Seq(AttrType("i", IntType()), AttrType("f", FloatType())))), Nil),
          "unknownrecords" -> MemoryLocation(SetType(RecordType(scala.collection.immutable.Seq(AttrType("dead", AnyType()), AttrType("alive", AnyType())))), Nil)
        ))
      assert(process(world, query) === t)
    }
  }

  checkTopType("for (r <- integers) yield set r + 1", SetType(IntType()))
  checkTopType("for (r <- unknown) yield set r + 1", SetType(IntType()))
  checkTopType("for (r <- unknown) yield set r + 1.0", SetType(FloatType()))
  checkTopType("for (r <- unknown; x <- integers; r = x) yield set r", SetType(IntType()))
  checkTopType("for (r <- unknown; x <- integers; r + x = 2*x) yield set r", SetType(IntType()))
  checkTopType("for (r <- unknown; x <- floats; r + x = x) yield set r", SetType(FloatType()))
  checkTopType("for (r <- unknown; x <- booleans; r and x) yield set r", SetType(BoolType()))
  checkTopType("for (r <- unknown; x <- strings; r = x) yield set r", SetType(StringType()))
  checkTopType("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))", IntType())
  checkTopType("for (r <- unknownrecords) yield set r.dead or r.alive", SetType(BoolType()))
  checkTopType("for (r <- unknownrecords, r.dead or r.alive) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("dead", BoolType()), AttrType("alive", BoolType())))), "data source record type is not inferred")
  checkTopType("for (r <- unknown, ((r.age + r.birth) > 2015) = r.alive) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType())))), "data source record type is not inferred")
  checkTopType("for (r <- unknown, (for (x <- integers) yield set r > x) = true) yield set r", SetType(IntType()))
  checkTopType("for (r <- unknown, (for (x <- records) yield set (r.value > x.f)) = true) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("value", FloatType())))), "mission record type inferrence")
  checkTopType("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)", SetType(IntType()))

  checkTopType("{ (a,b) := (1, 2); a+b }", IntType())
  checkTopType("{ (a,b) := (1, 2.2); a }", IntType())
  checkTopType("{ (a,b) := (1, 2.2); b }", FloatType())
  checkTopType("{ ((a,b),c) := ((1, 2.2), 3); a }", IntType())
  checkTopType("{ ((a,b),c) := ((1, 2.2), 3); b }", FloatType())
  checkTopType("{ ((a,b),c) := ((1, 2.2), 3); c }", IntType())
  checkTopType("{ ((a,b),c) := ((1, 2.2), 3); a+c }", IntType())
  checkTopType("{ (a,(b,c)) := (1, (2.2, 3)); a }", IntType())
  checkTopType("{ (a,(b,c)) := (1, (2.2, 3)); b }", FloatType())
  checkTopType("{ (a,(b,c)) := (1, (2.2, 3)); c }", IntType())
  checkTopType("{ (a,(b,c)) := (1, (2.2, 3)); a+c }", IntType())

  checkTopType("for ((a, b) <- list((1, 2.2))) yield set (a, b)", SetType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", FloatType())))))

  //  test("bad patterns") {
//   """{ (a, b, c) := (1, 2); a + b + c }""" should produce a message with incompatible pattern... how to test exact error mssage? or approximate error message?
//  }

}