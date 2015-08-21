package raw
package calculus

class SemanticAnalyzerTest extends FunTest {

  def success(query: String, world: World, tipe: Type) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)

    val analyzer = new SemanticAnalyzer(t, world)
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)
    analyzer.debugTreeTypes

    assert(analyzer.tipe(ast) === tipe)
  }

  def failure(query: String, world: World, error: String) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)

    val analyzer = new SemanticAnalyzer(t, world)
    analyzer.errors.foreach(err => logger.error(err.toString))

    assert(analyzer.errors.count{ case e => e.toString.contains(error) } > 0, s"Error '$error' not contained in errors")
  }

  test("cern") {
    success(
      "for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)", TestWorlds.cern,
      SetType(RecordType(List(AttrType("muon", RecordType(List(AttrType("pt", FloatType()), AttrType("eta", FloatType())), None))), None)))
  }

  test("departments1") {
    success(
      """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""", TestWorlds.departments,
      SetType(RecordType(List(AttrType("name", StringType()), AttrType("address", RecordType(List(AttrType("street", StringType()), AttrType("zipcode", StringType())), None))), None)))
  }

  test("departments2") {
    success(
      """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""", TestWorlds.departments,
      SetType(RecordType(List(AttrType("deptName", StringType())), None)))
  }

  test("employees") {
    success(
      "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)", TestWorlds.employees,
      SetType(RecordType(List(AttrType("E", RecordType(List(AttrType("dno", IntType()), AttrType("children", ListType(RecordType(List(AttrType("age", IntType())), None))), AttrType("manager", RecordType(List(AttrType("name", StringType()), AttrType("children", ListType(RecordType(List(AttrType("age", IntType())), None)))), None))), None)), AttrType("M", IntType())), None)))
  }

  test("simple type inference") {
    val world = new World(sources=Map(
      "unknown" -> SetType(TypeVariable(new Variable())),
      "integers" -> SetType(IntType()),
      "floats" -> SetType(FloatType()),
      "booleans" -> SetType(BoolType()),
      "strings" -> SetType(StringType()),
      "records" -> SetType(RecordType(List(AttrType("i", IntType()), AttrType("f", FloatType())), None)),
      "unknownrecords" -> SetType(RecordType(List(AttrType("dead", AnyType()), AttrType("alive", AnyType())), None))))

    success("for (r <- integers) yield set r + 1", world, SetType(IntType()))
    success("for (r <- unknown) yield set r + 1", world, SetType(IntType()))
    success("for (r <- unknown) yield set r + 1.0", world, SetType(FloatType()))
    success("for (r <- unknown; x <- integers; r = x) yield set r", world, SetType(IntType()))
    success("for (r <- unknown; x <- integers; r + x = 2*x) yield set r", world, SetType(IntType()))
    success("for (r <- unknown; x <- floats; r + x = x) yield set r", world, SetType(FloatType()))
    success("for (r <- unknown; x <- booleans; r and x) yield set r", world, SetType(BoolType()))
    success("for (r <- unknown; x <- strings; r = x) yield set r", world, SetType(StringType()))
    success("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))", world, IntType())
    success("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r", world, SetType(IntType()))
    // TODO: What do we want exactly to be the behaviour of 'v' in the following? Could be int or float. Or force it?
    //success("""for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""", world, SetType(IntType()))
    success("for (r <- unknown; v := r) yield set (r + 0)", world, SetType(IntType()))
    success("for (r <- unknownrecords) yield set r.dead or r.alive", world, SetType(BoolType()))
    success("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)", world, SetType(IntType()))
    success("{ (a,b) := (1, 2); a+b }", world, IntType())
    success("{ (a,b) := (1, 2.2); a }", world, IntType())
    success("{ (a,b) := (1, 2.2); b }", world, FloatType())
    success("{ ((a,b),c) := ((1, 2.2), 3); a }", world, IntType())
    success("{ ((a,b),c) := ((1, 2.2), 3); b }", world, FloatType())
    success("{ ((a,b),c) := ((1, 2.2), 3); c }", world, IntType())
    success("{ ((a,b),c) := ((1, 2.2), 3); a+c }", world, IntType())
    success("{ (a,(b,c)) := (1, (2.2, 3)); a }", world, IntType())
    success("{ (a,(b,c)) := (1, (2.2, 3)); b }", world, FloatType())
    success("{ (a,(b,c)) := (1, (2.2, 3)); c }", world, IntType())
    success("{ (a,(b,c)) := (1, (2.2, 3)); a+c }", world, IntType())
    success("{ x := for (i <- integers) yield set i; x }", world, SetType(IntType()))
    success("{ x := for (i <- integers) yield set i; for (y <- x) yield set y }", world, SetType(IntType()))
    success("{ z := 42; x := for (i <- integers) yield set i; for (y <- x) yield set y }", world, SetType(IntType()))
    success("{ z := 42; x := for (i <- integers; i = z) yield set i; for (y <- x) yield set y }", world, SetType(IntType()))
  }

  ignore("complex type inference") {
    val world = new World(sources=Map(
      "unknown" -> SetType(AnyType()),
      "unknownrecords" -> SetType(RecordType(List(AttrType("dead", AnyType()), AttrType("alive", AnyType())), None))))

    // TODO: Data source record type is not inferred
    success("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", world, SetType(RecordType(List(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType())), None)))
    // TODO: Missing record type inference
    success("for (r <- unknown; (for (x <- records) yield set (r.value > x.f)) = true) yield set r", world, SetType(RecordType(List(AttrType("value", FloatType())), None)))
    // TODO: Data source record type is not inferred
    success("for (r <- unknownrecords; r.dead or r.alive) yield set r", world, SetType(RecordType(List(AttrType("dead", BoolType()), AttrType("alive", BoolType())), None)))
  }

  test("expression block with multiple comprehensions") {
    val world = new World(sources = Map("integers" -> SetType(IntType())))

    success("""
    {
      z := 42;
      desc := "c.description";
      x := for (i <- integers; i = z) yield set i;
      for (y <- x) yield set 1
    }
    """, world, SetType(IntType()))
  }

  test("inference for function abstraction") {
    val world = new World()

    success("""\a: int -> a + 2""", world, FunType(IntType(), IntType()))
    success("""\a: int -> a""", world, FunType(IntType(), IntType()))
    success("""\a -> a + 2""", world, FunType(IntType(), IntType()))
    success("""\a -> a + a + 2""", world, FunType(IntType(), IntType()))
    success("""\a -> a""", world, FunType(AnyType(), AnyType()))
    success("""\(a: int, b: int) -> a + b + 2""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
    success("""\(a, b) -> a + b + 2""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
  }

  test("patterns") {
    val world = new World()

    success("""for ((a, b) <- list((1, 2.2))) yield set (a, b)""", world, SetType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", FloatType())), None)))
  }

  test("errors") {
    failure("for (t <- things; t.a + 1.0 > t.b ) yield set t.a", TestWorlds.things, "expected int got float")
  }

  test("propagate named records") {
    val student = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Student"))
    val students = ListType(student)
    val professor = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Professors"))
    val professors = ListType(professor)
    val world = new World(sources = Map("students" -> students, "professors" -> professors))

    success("for (s <- students) yield list s", world, students)
    success("for (s <- students) yield list (a := 1, b := s)", world, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", student)), None)))
    success("for (s <- students; p <- professors; s = p) yield list s", world, students)
    success("for (s <- students; p <- professors; s = p) yield list p", world, professors)
    success("for (s <- students; p <- professors; s = p) yield list (name := s.name, age := p.age)", world, ListType(RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), None)))
    success("for (s <- students; p <- professors) yield list (a := 1, b := s, c := p)", world, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", student), AttrType("c", professor)), None)))
  }
}