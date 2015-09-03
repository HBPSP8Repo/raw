package raw
package calculus

class SemanticAnalyzerTest extends FunTest {

  import raw.calculus.Calculus.{IdnDef, IdnUse}

  def go(query: String, world: World) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)
    logger.debug(s"Parsed tree: ${CalculusPrettyPrinter(t.root)}")

    val analyzer = new SemanticAnalyzer(t, world)
    analyzer.errors.foreach(e => logger.error(ErrorsPrettyPrinter(e)))
    analyzer
  }

  def success(query: String, world: World, tipe: Type) = {
    val analyzer = go(query, world)
    assert(analyzer.errors.isEmpty)
    logger.debug(s"Actual type: ${TypesPrettyPrinter(analyzer.tipe(analyzer.tree.root))}")
    logger.debug(s"Expected type: ${TypesPrettyPrinter(tipe)}")
    compare(TypesPrettyPrinter(analyzer.tipe(analyzer.tree.root)), TypesPrettyPrinter(tipe))
  }

  def failure(query: String, world: World, error: Error) = {
    val analyzer = go(query, world)
    assert(analyzer.errors.nonEmpty)
    logger.debug(analyzer.errors.toString)
    error match {

      //
      // Test case helpers
      //

      // Incompatible types can be t1,t2 or t2, t1
      case IncompatibleTypes(t1, t2) =>
        assert(analyzer.errors.exists{
          case IncompatibleTypes(`t1`, `t2`) => true
          case IncompatibleTypes(`t2`, `t1`) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      // Ignore text description in expected types, even if defined
      case UnexpectedType(t, expected, _) =>
        assert(analyzer.errors.exists{
          case UnexpectedType(`t`, `expected`, _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      case _ =>
        assert(analyzer.errors.exists{
          case `error` => true
          case _ => false }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
    }
  }

  test("cern") {
    success("for (e <- Events) yield list e", TestWorlds.cern, TestWorlds.cern.sources("Events"))
    success("for (e <- Events) yield set e", TestWorlds.cern, SetType(TestWorlds.cern.sources("Events").asInstanceOf[ListType].innerType))
    success("for (e <- Events; m <- e.muons) yield set m", TestWorlds.cern, SetType(RecordType(List(AttrType("pt",FloatType()), AttrType("eta",FloatType())),None)))
    success("for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)", TestWorlds.cern, SetType(RecordType(List(AttrType("muon", RecordType(List(AttrType("pt", FloatType()), AttrType("eta", FloatType())), None))), None)))
  }

  test("linkedList") {
    success("for (i <- Items) yield set i", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
    success("for (i <- Items) yield set i.next", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
    success("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
  }

  test("departments1") {
    success(
      """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""", TestWorlds.departments,
      SetType(RecordType(List(AttrType("name", StringType()), AttrType("address", RecordType(List(AttrType("street", StringType()), AttrType("zipcode", StringType())), None))), None)))
  }

  test("departments2") {
    success("""for (d <- Departments) yield set d""", TestWorlds.departments, SetType(UserType(Symbol("Department"))))
    success(
      """for ( d <- Departments; d.name = "CSE") yield set d""", TestWorlds.departments,
      SetType(UserType(Symbol("Department"))))
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
      "unknown" -> SetType(TypeVariable()),
      "integers" -> SetType(IntType()),
      "floats" -> SetType(FloatType()),
      "booleans" -> SetType(BoolType()),
      "strings" -> SetType(StringType()),
      "records" -> SetType(RecordType(List(AttrType("i", IntType()), AttrType("f", FloatType())), None)),
      "unknownrecords" -> SetType(RecordType(List(AttrType("dead", AnyType()), AttrType("alive", AnyType())), None))))


    success("for (r <- integers) yield max r", world, IntType())
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

    success("""for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""", world, SetType(IntType()))

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

  test("complex type inference") {
    val world = new World(sources=Map(
      "unknown" -> SetType(TypeVariable()),
      "unknownrecords" -> SetType(RecordType(List(AttrType("dead", TypeVariable()), AttrType("alive", TypeVariable())), None))))

    success("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", world, SetType(ConstraintRecordType(Set(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType())))))
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

    success("""\a -> a + 2""", world, FunType(IntType(), IntType()))
    success("""\a -> a + a + 2""", world, FunType(IntType(), IntType()))
    success("""\(a, b) -> a + b + 2""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
    val a = TypeVariable()
    success("""\a -> a""", world, FunType(a, a))
    success("""\x -> x.age + 2""", world, FunType(ConstraintRecordType(Set(AttrType("age", IntType()))), IntType()))
    failure("""\(x, y) -> x + y""", world, TooManySolutions)

//     TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//    success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
//      FunType(
//        ConstraintCollectionType(ConstraintRecordType(Set(AttrType("age", IntType()), AttrType("name", TypeVariable()))), None, None),
//        BagType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", TypeVariable())), None))))
  }

  test("patterns") {
    val world = new World()

    success("""for ((a, b) <- list((1, 2.2))) yield set (a, b)""", world, SetType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", FloatType())), None)))
  }

  test("errors") {
    failure("1 + 1.", new World(), IncompatibleTypes(IntType(), FloatType()))
    failure("1 - 1.", new World(), IncompatibleTypes(IntType(), FloatType()))
    failure("1 + true", new World(), IncompatibleTypes(IntType(), BoolType()))
    failure("1 - true", new World(), IncompatibleTypes(IntType(), BoolType()))
    failure("1 + things", TestWorlds.things, IncompatibleTypes(IntType(), TestWorlds.things.sources("things")))
    failure("1 - things", TestWorlds.things, IncompatibleTypes(IntType(), TestWorlds.things.sources("things")))
    failure("for (t <- things; t.a > 10.23) yield and true", TestWorlds.things, IncompatibleTypes(IntType(), FloatType()))
    failure("for (t <- things; t.a + 1.0 > t.b ) yield set t.a", TestWorlds.things, IncompatibleTypes(IntType(), FloatType()))
    failure("a + 1", new World(), UnknownDecl(IdnUse("a")))
    failure("{ a := 1; a := 2; a }", new World(), MultipleDecl(IdnDef("a")))
    failure("for (a <- blah) yield set a", new World(), UnknownDecl(IdnUse("blah")))
    failure("for (a <- things) yield set b", TestWorlds.things, UnknownDecl(IdnUse("b")))
    failure("for (a <- things; a <- things) yield set a", TestWorlds.things, MultipleDecl(IdnDef("a")))
    failure("if 1 then 1 else 0", new World(), UnexpectedType(IntType(), BoolType(), None))
    failure("if true then 1 else 1.", new World(), IncompatibleTypes(IntType(), FloatType()))
    //    failure("{ a := 1; b := 1.; c := 2; d := 2.; (a + b) + (c + d)")
  }

  test("propagate named records") {
    val student = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Student"))
    val students = ListType(student)
    val professor = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Professors"))
    val professors = ListType(professor)
    val world = new World(sources = Map("students" -> students, "professors" -> professors))

    success("for (s <- students) yield list s", world, students)
//    success("for (s <- students) yield list (a := 1, b := s)", world, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", student)), None)))
    // TODO: Check w/ Ben. The following should indeed blow up!
//    success("for (s <- students; p <- professors; s = p) yield list s", world, students)
//    success("for (s <- students; p <- professors; s = p) yield list p", world, professors)
//    success("for (s <- students; p <- professors; s = p) yield list (name := s.name, age := p.age)", world, ListType(RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), None)))
    success("for (s <- students; p <- professors) yield list (a := 1, b := s, c := p)", world, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", student), AttrType("c", professor)), None)))
  }

  test("soundness") {
    val world = new World()

    // TODO: The following is unsound, as x and y are inferred to be AnyType() - but not the same type - so things can break.
    // TODO: The 'walk' tree has the variables pointing to the same thing, but we loose that, due to how we do/don't do constraints.
    success("""\(x, y) -> x + y + 10""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
    success("""\(x, y) -> x + y + 10.2""", world, FunType(RecordType(List(AttrType("_1", FloatType()), AttrType("_2", FloatType())), None), FloatType()))
//    success("""\(x, y) -> x.age = y""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))

//    success("""\(x, y) -> (x, y)""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))

//    success("""\(x, y) -> if (x = y) then x else y""", world, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
  }

  test("coolness") {
    val student = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Student"))
    val students = ListType(student)
    val professor = RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), Some("Professors"))
    val professors = ListType(professor)
    val world = new World(sources = Map("students" -> students, "professors" -> professors))

    // TODO: Aren't we binding too much? students to sum1?
    val z = TypeVariable()
    val yz = TypeVariable()
    success("""\(x,y) -> for (z <- x) yield sum y(z)""", world,
      FunType(
        RecordType(List(AttrType("_1",ConstraintCollectionType(z, None, None)), AttrType("_2", FunType(z,yz))), None),
        yz))

//    success(
//      """
//        {
//        sum1 := \(x,y) -> for (z <- x) yield sum y(z);
//        age := (students, \x -> x.age);
//
//        v := sum1(age);
//        sum1
//        }
//
//      """, world,

    // List of issues left:
    // 1) Issue above is that we are binding too much. Causes problems in libraries
    // 2) Issue below is that we return a too broad type (TypeVariable) to represent int/float and that will blow up at runtime with the wrong types
    // even though the program type checked. So, it's unsound. NO. This is NOT AN ISSUE. We return multiple solutions and that is correct.
    // 3) We don't return unrelated errors.
    // 4) We don't support recursive types.
    // 5) Do we support recursive functions? YES, in the solver, but we need new parser nodes to represent it and their chain/scoping rules.

    // whenever we have a function application,
    // we need to copy its constraints

    // if we add a semantic check that if smtg is TypeVariable, we make it sound, but we then must indeed bind 'too much'
    // otherwise funabs cannot return type variables.

//    success(
//      """
//        {
//        sum1 := \(x,y) -> for (z <- x) yield sum y(z);
//        sum1
//        }
//
//      """, world,
//      FunType(
//        RecordType(List(AttrType("_1",students), AttrType("_2", FunType(student,IntType()))), None),
//        IntType()))
  }

  test("recursive lambda function") {
    // TODO: Add syntax like: fun f(x) -> if (x = 0) then 1 else f(x - 1) * x)
    // TODO: and we really showed that it should simply type to FunType(IntType(), IntType().
    // TODO: The rest is just code generation gymnics

    success(
      """
         {
        recursive := (\(f, arg) -> f(arg));
        factorial := (\n -> recursive( (\(rec, v) -> if (v = 0) then 1 else rec(v - 1) * v), n));
        factorial
        }
      """, new World(), FunType(IntType(), IntType()))

  }

}