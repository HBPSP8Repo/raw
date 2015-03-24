package raw
package calculus

class SemanticAnalyzerTest extends FunTest {

  def assertRootType(query: String, tipe: Type, issue: String = "", world: World = null) = {
    val run = (d: String) => if (issue != "") ignore(d + s" ($issue)") _ else test(d) _
    run(query) {
      val w: World =
        if (world != null)
          world
        else
          new World(sources=Map(
            "unknown" -> SetType(AnyType()),
            "integers" -> SetType(IntType()),
            "floats" -> SetType(FloatType()),
            "booleans" -> SetType(BoolType()),
            "strings" -> SetType(StringType()),
            "records" -> SetType(RecordType(scala.collection.immutable.Seq(AttrType("i", IntType()), AttrType("f", FloatType())))),
            "unknownrecords" -> SetType(RecordType(scala.collection.immutable.Seq(AttrType("dead", AnyType()), AttrType("alive", AnyType()))))))

      val ast = parse(query)
      val analyzer = new SemanticAnalyzer(new Calculus.Calculus(ast), w)
      analyzer.errors.map((err) => logger.error(err.toString))
      assert(analyzer.errors.length === 0)
      analyzer.debugTreeTypes

      assert(analyzer.tipe(ast) === tipe)
    }
  }

  def assertFailure(query: String, error: String, world: World) = {
    test(query) {
      val ast = parse(query)
      val analyzer = new SemanticAnalyzer(new Calculus.Calculus(ast), world)
      analyzer.errors.map((err) => logger.error(err.toString))

      assert(analyzer.errors.count{ case e => e.toString.contains(error) } > 0, s"Error '$error' not contained in errors")
    }
  }

  assertRootType(
    "for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)",
    SetType(RecordType(List(AttrType("muon", RecordType(List(AttrType("pt", FloatType()), AttrType("eta", FloatType()))))))),
    world=TestWorlds.cern)

  assertRootType(
    """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""",
    SetType(RecordType(List(AttrType("name", StringType()),AttrType("address", RecordType(List(AttrType("street", StringType()),AttrType("zipcode", StringType()))))))),
    world=TestWorlds.departments)

  assertRootType(
    "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)",
    SetType(RecordType(List(AttrType("E",RecordType(List(AttrType("children", ListType(RecordType(List(AttrType("age", IntType()))))),AttrType("manager", RecordType(List(AttrType("name", StringType()),AttrType("children", ListType(RecordType(List(AttrType("age", IntType()))))))))))),AttrType("M", IntType())))),
    world=TestWorlds.employees)

  assertRootType(
    """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""",
    SetType(RecordType(List(AttrType("deptName", StringType())))),
    world=TestWorlds.departments)

  assertRootType("for (r <- integers) yield set r + 1", SetType(IntType()))
  assertRootType("for (r <- unknown) yield set r + 1", SetType(IntType()))
  assertRootType("for (r <- unknown) yield set r + 1.0", SetType(FloatType()))
  assertRootType("for (r <- unknown; x <- integers; r = x) yield set r", SetType(IntType()))
  assertRootType("for (r <- unknown; x <- integers; r + x = 2*x) yield set r", SetType(IntType()))
  assertRootType("for (r <- unknown; x <- floats; r + x = x) yield set r", SetType(FloatType()))
  assertRootType("for (r <- unknown; x <- booleans; r and x) yield set r", SetType(BoolType()))
  assertRootType("for (r <- unknown; x <- strings; r = x) yield set r", SetType(StringType()))
  assertRootType("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))", IntType())
  assertRootType("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType())))), "data source record type is not inferred")
  assertRootType("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r", SetType(IntType()))
  assertRootType("for (r <- unknown; (for (x <- records) yield set (r.value > x.f)) = true) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("value", FloatType())))), "missing record type inference")
  assertRootType("""for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""", SetType(IntType()))
  assertRootType("for (r <- unknown; v := r) yield set (r + 0)", SetType(IntType()))
  assertRootType("for (r <- unknownrecords) yield set r.dead or r.alive", SetType(BoolType()))
  assertRootType("for (r <- unknownrecords; r.dead or r.alive) yield set r", SetType(RecordType(scala.collection.immutable.Seq(AttrType("dead", BoolType()), AttrType("alive", BoolType())))), "data source record type is not inferred")
  assertRootType("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)", SetType(IntType()))

  assertRootType("{ (a,b) := (1, 2); a+b }", IntType())
  assertRootType("{ (a,b) := (1, 2.2); a }", IntType())
  assertRootType("{ (a,b) := (1, 2.2); b }", FloatType())
  assertRootType("{ ((a,b),c) := ((1, 2.2), 3); a }", IntType())
  assertRootType("{ ((a,b),c) := ((1, 2.2), 3); b }", FloatType())
  assertRootType("{ ((a,b),c) := ((1, 2.2), 3); c }", IntType())
  assertRootType("{ ((a,b),c) := ((1, 2.2), 3); a+c }", IntType())
  assertRootType("{ (a,(b,c)) := (1, (2.2, 3)); a }", IntType())
  assertRootType("{ (a,(b,c)) := (1, (2.2, 3)); b }", FloatType())
  assertRootType("{ (a,(b,c)) := (1, (2.2, 3)); c }", IntType())
  assertRootType("{ (a,(b,c)) := (1, (2.2, 3)); a+c }", IntType())

  assertRootType("""\a: int -> a + 2""", FunType(IntType(), IntType()))
  assertRootType("""\a: int -> a""", FunType(IntType(), IntType()))
  assertRootType("""\a -> a + 2""", FunType(IntType(), IntType()))
  assertRootType("""\a -> a + a + 2""", FunType(IntType(), IntType()))
  assertRootType("""\a -> a""", FunType(AnyType(), AnyType()))

  assertRootType("""\(a: int, b: int) -> a + b + 2""", FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType()))), IntType()))
  assertRootType("""\(a, b) -> a + b + 2""", FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType()))), IntType()))

//  assertRootType("for ((a, b) <- list((1, 2.2))) yield set (a, b)", SetType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", FloatType())))))
//
//  assertFailure("for (t <- things; t.a + 1.0 > t.b ) yield set t.a", "expected int got float", TestWorlds.things)
}