package raw
package calculus

class SemanticAnalyzerTest extends FunTest {

  import raw.calculus.Calculus._

  def go(query: String, world: World) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)
    logger.debug(s"AST: ${t.root}")
    logger.debug(s"Parsed tree: ${CalculusPrettyPrinter(t.root)}")

    val analyzer = new SemanticAnalyzer(t, world, Some(query))
    analyzer.errors.foreach{ case err => logger.debug(s"Error: ${ErrorsPrettyPrinter(err)}")}
    analyzer
  }

  def success(query: String, world: World, expectedType: Type) = {
    val analyzer = go(query, world)
    assert(analyzer.errors.isEmpty)
    val inferredType = analyzer.tipe(analyzer.tree.root)
    analyzer.errors.foreach{ case err => logger.debug(s"Error: ${ErrorsPrettyPrinter(err)}")}
    analyzer.printTypedTree()
    logger.debug(s"Actual type: ${FriendlierPrettyPrinter(inferredType)}")
    logger.debug(s"Expected type: ${FriendlierPrettyPrinter(expectedType)}")
    assert(compare(inferredType.toString, expectedType.toString))
    assert(typesEq(inferredType, expectedType))
  }

  private def typesEq(t1: Type, t2: Type): Boolean =
    compare(PrettyPrinter(t1), PrettyPrinter(t2))
  
  def failure(query: String, world: World, error: Error) = {
    val analyzer = go(query, world)
    assert(analyzer.errors.nonEmpty)
    analyzer.errors.foreach{ case err => logger.debug(s"Error: ${ErrorsPrettyPrinter(err)}")}
    error match {

      //
      // Test case helpers: e..g ignore positions
      //

      case PatternMismatch(pat, t, _) =>
        assert(analyzer.errors.exists {
          case PatternMismatch(`pat`, `t`, _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      // Incompatible types can be specified in the tests in either order
      // Positions are also ignored
      case IncompatibleTypes(t1, t2, _, _) =>
        assert(analyzer.errors.exists {
          case IncompatibleTypes(ta, tb, _, _) if typesEq(t1, ta) && typesEq(t2, tb) => true
          case IncompatibleTypes(tb, ta, _, _) if typesEq(t1, ta) && typesEq(t2, tb) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      // Ignore text description and position
      case UnexpectedType(t, expected, _, _) =>
        assert(analyzer.errors.exists {
          case UnexpectedType(t1, expected1, _, _) if typesEq(t, t1) && typesEq(expected, expected1) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      case IncompatibleMonoids(m, t, _) =>
        assert(analyzer.errors.exists {
          case IncompatibleMonoids(`m`, `t`, _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      case _ =>
        assert(analyzer.errors.exists {
          case `error` => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
    }
  }

  test("for (e <- Events) yield list e") {
    success("for (e <- Events) yield list e", TestWorlds.cern, TestWorlds.cern.sources("Events"))
  }

//  test("for (e <- Events) yield set e") {
//    success("for (e <- Events) yield set e", TestWorlds.cern, CollectionType(SetMonoid(),TestWorlds.cern.sources("Events").asInstanceOf[Type].innerType))
//  }

  test("for (e <- Events; m <- e.muons) yield set m") {
    success("for (e <- Events; m <- e.muons) yield set m", TestWorlds.cern, CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("pt", FloatType()), AttrType("eta", FloatType()))))))
  }

  test("for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon: m)") {
    success("for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon: m)", TestWorlds.cern, CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("muon", RecordType(Attributes(List(AttrType("pt", FloatType()), AttrType("eta", FloatType()))))))))))
  }

  test("for (i <- Items) yield set i") {
    success("for (i <- Items) yield set i", TestWorlds.linkedList, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
  }

  test("for (i <- Items) yield set i.next") {
    success("for (i <- Items) yield set i.next", TestWorlds.linkedList, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
  }

  test("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x") {
    success("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x", TestWorlds.linkedList, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
  }

  test("departments - from Fegaras's paper") {
    success(
      """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name: e.name, address: e.address)""", TestWorlds.departments,
      CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("name", StringType()), AttrType("address", RecordType(Attributes(List(AttrType("street", StringType()), AttrType("zipcode", StringType()))))))))))
  }

  test("for (d <- Departments) yield set d") {
    success( """for (d <- Departments) yield set d""", TestWorlds.departments, CollectionType(SetMonoid(),UserType(Symbol("Department"))))
  }

  test( """for ( d <- Departments; d.name = "CSE") yield set d""") {
    success( """for ( d <- Departments; d.name = "CSE") yield set d""", TestWorlds.departments, CollectionType(SetMonoid(),UserType(Symbol("Department"))))
  }

  test( """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName: name) }""") {
    success( """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName: name) }""", TestWorlds.departments, CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("deptName", StringType()))))))
  }

  test("employees - from Fegaras's paper") {
    success(
      "for (e <- Employees) yield set (E: e, M: for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)", TestWorlds.employees,
      CollectionType(SetMonoid(),
        RecordType(Attributes(List(
          AttrType("E",
            RecordType(Attributes(List(
              AttrType("dno", IntType()),
              AttrType("children", CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("age", IntType())))))),
              AttrType("manager", RecordType(Attributes(List(
                AttrType("name", StringType()),
                AttrType("children", CollectionType(ListMonoid(),RecordType(Attributes(List(
                  AttrType("age", IntType())))))))))))))),
          AttrType("M", IntType()))))))
  }

  test("for (r <- integers) yield max r") {
    success("for (r <- integers) yield max r", TestWorlds.simple, IntType())
  }

  test("for (r <- integers) yield set r + 1") {
    success("for (r <- integers) yield set r + 1", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("for (r <- unknown) yield set r + 1") {
    success("for (r <- unknown) yield set r + 1", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("for (r <- unknown) yield set r + 1.0") {
    success("for (r <- unknown) yield set r + 1.0", TestWorlds.simple, CollectionType(SetMonoid(),FloatType()))
  }

  test("for (r <- unknown; x <- integers; r = x) yield set r") {
    success("for (r <- unknown; x <- integers; r = x) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("for (r <- unknown; x <- integers; r + x = 2*x) yield set r") {
    success("for (r <- unknown; x <- integers; r + x = 2*x) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("for (r <- unknown; x <- floats; r + x = x) yield set r") {
    success("for (r <- unknown; x <- floats; r + x = x) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),FloatType()))
  }

  test("for (r <- unknown; x <- booleans; r and x) yield set r") {
    success("for (r <- unknown; x <- booleans; r and x) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),BoolType()))
  }

  test("for (r <- unknown; x <- strings; r = x) yield set r") {
    success("for (r <- unknown; x <- strings; r = x) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),StringType()))
  }

  test("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))") {
    success("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))", TestWorlds.simple, IntType())
  }

  test("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r") {
    success("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test( """for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""") {
    success( """for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("for (r <- unknown; v := r) yield set (r + 0)") {
    success("for (r <- unknown; v := r) yield set (r + 0)", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("bug") {
    success("for (s <- unknown; a := s + 2) yield set s", TestWorlds.unknown, CollectionType(SetMonoid(), IntType()))
  }

  test("{ a:= -unknownvalue; unknownvalue}") {
    success("{ a:= -unknownvalue; unknownvalue}", TestWorlds.unknown, NumberType())
  }

  test("{ a:= not unknownvalue; unknownvalue}") {
    success("{ a:= not unknownvalue; unknownvalue}", TestWorlds.unknown, BoolType())
  }

  test("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)") {
    success("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("{ (a,b) := (1, 2); a+b }") {
    success("{ (a,b) := (1, 2); a+b }", TestWorlds.simple, IntType())
  }

  test("{ (a,b) := (1, 2.2); a }") {
    success("{ (a,b) := (1, 2.2); a }", TestWorlds.simple, IntType())
  }

  test("{ (a,b) := (1, 2.2); b }") {
    success("{ (a,b) := (1, 2.2); b }", TestWorlds.simple, FloatType())
  }

  test("{ ((a,b),c) := ((1, 2.2), 3); a }") {
    success("{ ((a,b),c) := ((1, 2.2), 3); a }", TestWorlds.simple, IntType())
  }

  test("{ ((a,b),c) := ((1, 2.2), 3); b }") {
    success("{ ((a,b),c) := ((1, 2.2), 3); b }", TestWorlds.simple, FloatType())
  }

  test("{ ((a,b),c) := ((1, 2.2), 3); c }") {
    success("{ ((a,b),c) := ((1, 2.2), 3); c }", TestWorlds.simple, IntType())
  }

  test("{ ((a,b),c) := ((1, 2.2), 3); a+c }") {
    success("{ ((a,b),c) := ((1, 2.2), 3); a+c }", TestWorlds.simple, IntType())
  }

  test("{ (a,(b,c)) := (1, (2.2, 3)); a }") {
    success("{ (a,(b,c)) := (1, (2.2, 3)); a }", TestWorlds.simple, IntType())
  }

  test("{ (a,(b,c)) := (1, (2.2, 3)); b }") {
    success("{ (a,(b,c)) := (1, (2.2, 3)); b }", TestWorlds.simple, FloatType())
  }

  test("{ (a,(b,c)) := (1, (2.2, 3)); c }") {
    success("{ (a,(b,c)) := (1, (2.2, 3)); c }", TestWorlds.simple, IntType())
  }

  test("{ (a,(b,c)) := (1, (2.2, 3)); a+c }") {
    success("{ (a,(b,c)) := (1, (2.2, 3)); a+c }", TestWorlds.simple, IntType())
  }

  test("{ x := for (i <- integers) yield set i; x }") {
    success("{ x := for (i <- integers) yield set i; x }", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("{ x := for (i <- integers) yield set i; for (y <- x) yield set y }") {
    success("{ x := for (i <- integers) yield set i; for (y <- x) yield set y }", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("{ z := 42; x := for (i <- integers) yield set i; for (y <- x) yield set y }") {
    success("{ z := 42; x := for (i <- integers) yield set i; for (y <- x) yield set y }", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("{ z := 42; x := for (i <- integers; i = z) yield set i; for (y <- x) yield set y }") {
    success("{ z := 42; x := for (i <- integers; i = z) yield set i; for (y <- x) yield set y }", TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  ignore("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r") {
    success("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", TestWorlds.unknown, CollectionType(SetMonoid(),RecordType(AttributesVariable(Set(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType()))))))
  }

  test("for (r <- unknownrecords) yield set r.dead or r.alive") {
    success("for (r <- unknownrecords) yield set r.dead or r.alive", TestWorlds.unknown, CollectionType(SetMonoid(),BoolType()))
  }

  test("for (r <- unknownrecords; r.dead or r.alive) yield set r") {
    success("for (r <- unknownrecords; r.dead or r.alive) yield set r", TestWorlds.unknown, CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("dead", BoolType()), AttrType("alive", BoolType()))))))
  }

  test("expression block with multiple comprehensions") {
    success(
      """
    {
      z := 42;
      desc := "c.description";
      x := for (i <- integers; i = z) yield set i;
      for (y <- x) yield set 1
    }
    """, TestWorlds.simple, CollectionType(SetMonoid(),IntType()))
  }

  test("""\a -> a + 2""") {
    success( """\a -> a + 2""", TestWorlds.empty, FunType(IntType(), IntType()))
  }

  test( """\a -> a + a + 2""") {
    success( """\a -> a + a + 2""", TestWorlds.empty, FunType(IntType(), IntType()))
  }

  test( """\(a, b) -> a + b + 2""") {
    success( """\(a, b) -> a + b + 2""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(IntType()), PatternAttrType(IntType()))), IntType()))
  }

  test( """\a -> a""") {
    val a = TypeVariable()
    success( """\a -> a""", TestWorlds.empty, FunType(a, a))
  }

  test( """\x -> x.age + 2""") {
    success( """\x -> x.age + 2""", TestWorlds.empty, FunType(RecordType(AttributesVariable(Set(AttrType("age", IntType())))), IntType()))
  }

  test( """\(x, y) -> x + y""") {
    val n = NumberType()
    success( """\(x, y) -> x + y""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(n), PatternAttrType(n))), n))
  }

  test("""{ recursive := \(f, arg) -> f(arg); recursive } """) {
    var arg = TypeVariable()
    val out = TypeVariable()
    val f = FunType(arg, out)
    success( """{ recursive := \(f, arg) -> f(arg); recursive } """, TestWorlds.empty, FunType(PatternType(List(PatternAttrType(f), PatternAttrType(arg))), out))
  }

//     TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//    success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
//      FunType(
//        ConstraintCollectionType(ConstraintRecordType(Set(AttrType("age", IntType()), AttrType("name", TypeVariable())))),
//        CollectionType(BagMonoid(),RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("_2", TypeVariable()))))))

  test("for ((a, b) <- list((1, 2.2))) yield set (a, b)") {
    success("""for ((a, b) <- list((1, 2.2))) yield set (a, b)""", TestWorlds.empty, CollectionType(SetMonoid(),RecordType(Attributes(List(AttrType("a", IntType()), AttrType("b", FloatType()))))))
  }

  test("1 + 1.") {
    failure("1 + 1.", TestWorlds.empty, IncompatibleTypes(IntType(), FloatType()))
  }

  test("1 - 1.") {
    failure("1 - 1.", TestWorlds.empty, IncompatibleTypes(IntType(), FloatType()))
  }

  test("1 + true") {
    failure("1 + true", TestWorlds.empty, IncompatibleTypes(IntType(), BoolType()))
  }

  test("1 - true") {
    failure("1 - true", TestWorlds.empty, IncompatibleTypes(IntType(), BoolType()))
  }

  test("1 + things") {
    failure("1 + things", TestWorlds.things, IncompatibleTypes(IntType(), TestWorlds.things.sources("things")))
  }

  test("1 - things") {
    failure("1 - things", TestWorlds.things, IncompatibleTypes(IntType(), TestWorlds.things.sources("things")))
  }

  test("for (t <- things; t.a > 10.23) yield and true") {
    failure("for (t <- things; t.a > 10.23) yield and true", TestWorlds.things, IncompatibleTypes(IntType(), FloatType()))
  }

  test("for (t <- things; t.a + 1.0 > t.b ) yield set t.a") {
    failure("for (t <- things; t.a + 1.0 > t.b ) yield set t.a", TestWorlds.things, IncompatibleTypes(IntType(), FloatType()))
  }

  test("a") {
    failure("a", TestWorlds.empty, UnknownDecl(IdnUse("a")))
  }

  test("a + 1") {
    failure("a + 1", TestWorlds.empty, UnknownDecl(IdnUse("a")))
  }

  test("{ a := 1; a := 2; a }") {
    failure("{ a := 1; a := 2; a }", TestWorlds.empty, MultipleDecl(IdnDef("a")))
  }

  test("for (a <- blah) yield set a") {
    failure("for (a <- blah) yield set a", TestWorlds.empty, UnknownDecl(IdnUse("blah")))
  }

  test("for (a <- things) yield set b") {
    failure("for (a <- things) yield set b", TestWorlds.things, UnknownDecl(IdnUse("b")))
  }

  test("for (a <- things; a <- things) yield set a") {
    failure("for (a <- things; a <- things) yield set a", TestWorlds.things, MultipleDecl(IdnDef("a")))
  }

  test("if 1 then 1 else 0") {
    failure("if 1 then 1 else 0", TestWorlds.empty, UnexpectedType(IntType(), BoolType()))
  }

  test("if true then 1 else 1.") {
    failure("if true then 1 else 1.", TestWorlds.empty, IncompatibleTypes(IntType(), FloatType()))
  }

  test("{ a := 1; b := 1; c := 2.; (a + b) + c }") {
    failure("{ a := 1; b := 1; c := 2.; (a + b) + c }", TestWorlds.empty, IncompatibleTypes(IntType(), FloatType()))
  }

  test("{ a := 1; b := 1.; c := 2; d := 2.; (a + b) + (c + d) }") {
    failure("{ a := 1; b := 1.; c := 2; d := 2.; (a + b) + (c + d) }", TestWorlds.empty, IncompatibleTypes(IntType(), FloatType()))
  }

  test("for (t <- things) yield sum 1") {
    failure("for (t <- things) yield sum 1", TestWorlds.things, IncompatibleMonoids(SumMonoid(), TestWorlds.things.sources("things")))
  }

  test("for (t <- things) yield and true") {
    success("for (t <- things) yield and true", TestWorlds.things, BoolType())
  }

  test("for (t <- things) yield bag t") {
    failure("for (t <- things) yield bag t", TestWorlds.things, IncompatibleMonoids(BagMonoid(), TestWorlds.things.sources("things")))
  }

  test("for (t <- things) yield list t") {
    failure("for (t <- things) yield list t", TestWorlds.things, IncompatibleMonoids(ListMonoid(), TestWorlds.things.sources("things")))
  }

  test("for (s <- students) yield list s") {
    success("for (s <- students) yield list s", TestWorlds.professors_students, CollectionType(ListMonoid(),UserType(Symbol("student"))))
  }

  test("for (s <- students) yield list (a: 1, b: s)") {
    success("for (s <- students) yield list (a: 1, b: s)", TestWorlds.professors_students, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("a", IntType()), AttrType("b", UserType(Symbol("student"))))))))
  }

  test("for (s <- students; p <- professors; s = p) yield list s") {
    success("for (s <- students; p <- professors; s = p) yield list s", TestWorlds.professors_students, CollectionType(ListMonoid(), UserType(Symbol("student"))))
  }

  test("for (s <- students; p <- professors) yield list (name: s.name, age: p.age)") {
    success("for (s <- students; p <- professors) yield list (name: s.name, age: p.age)", TestWorlds.professors_students, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))
  }

  test("for (s <- students; p <- professors) yield list (a: 1, b: s, c: p)") {
    success("for (s <- students; p <- professors) yield list (a: 1, b: s, c: p)", TestWorlds.professors_students, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("a", IntType()), AttrType("b", UserType(Symbol("student"))), AttrType("c", UserType(Symbol("professor"))))))))
  }

  test("""\(x, y) -> x + y + 10""") {
    success("""\(x, y) -> x + y + 10""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(IntType()), PatternAttrType(IntType()))), IntType()))
  }

  test("""\(x, y) -> x + y + 10.2""") {
    success("""\(x, y) -> x + y + 10.2""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(FloatType()), PatternAttrType(FloatType()))), FloatType()))
  }

  test("""\(x, y) -> { z := x; y + z }""") {
    val n = NumberType()
    success("""\(x, y) -> { z := x; y + z }""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(n), PatternAttrType(n))), n))
  }

  test("""{ x := { y := 1; z := y; z }; x }""") {
    success("""{ x := { y := 1; z := y; z }; x }""", TestWorlds.empty, IntType())
  }

  test("""let polymorphism - not binding into functions""") {
    val m = GenericMonoid(idempotent=Some(false))
    val z = TypeVariable()
    val n = NumberType()
    success(
      """
        {
        sum1 := (\(x,y) -> for (z <- x) yield sum (y(z)));
        age := (students, \x -> x.age);
        v := sum1(age);
        sum1
        }

      """, TestWorlds.professors_students,
      FunType(RecordType(Attributes(List(AttrType("_1", CollectionType(m, z)), AttrType("_2", FunType(z, n))))), n))
  }

  test("""home-made count applied to wrong type""") {
    val m =
    failure(
      """
        {
        count1 := \x -> for (z <- x) yield sum 1
        count1(1)
        }

      """, TestWorlds.empty,
      UnexpectedType(FunType(CollectionType(GenericMonoid(idempotent=Some(false)), TypeVariable()), IntType()), FunType(IntType(), TypeVariable()))
    )
  }

  test("""let-polymorphism #1""") {
    success(
      """
        {
          f := \x -> x;
          (f(1), f(true))
        }
      """, TestWorlds.empty, RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("_2", BoolType())))))
  }

  test("""let-polymorphism #2""") {
    success(
      """
        {
          f := \x -> (x, 12);
          (f(1), f(true))
        }
      """, TestWorlds.empty,
      RecordType(Attributes(List(
        AttrType("_1", RecordType(Attributes(List(AttrType("x",  IntType()), AttrType("_2", IntType()))))),
        AttrType("_2", RecordType(Attributes(List(AttrType("x",  BoolType()), AttrType("_2", IntType())))))))))
  }

  test("""let-polymorphism #3""") {
    val i = TypeVariable()
    success(
    """ {
      f := \x -> x;
      g := \y -> y;
      h := if (true) then f else g;
      h
}
    """, TestWorlds.empty, FunType(i, i))
  }

  test("""let polymorphism #4""") {
    val z = TypeVariable()
    val n = NumberType()
    success(
      """
        {
        x := 1;
        f := \v -> v = x;
        v := f(10);
        f
        }

      """, TestWorlds.empty,
      FunType(IntType(), BoolType()))
  }

  test("""let-polymorphism #5""") {
    val x = TypeVariable()
    val age = TypeVariable()
    val rec = RecordType(AttributesVariable(Set(AttrType("age", age))))
    val rec2 = RecordType(AttributesVariable(Set(AttrType("age", BoolType()))))
    success(
      """
        {
        f := \x -> x = x;
        g := \x -> x.age;
        (f, g, if true then f else g)
        }
      """, TestWorlds.empty, RecordType(Attributes(List(
      AttrType("f", FunType(x, BoolType())),
      AttrType("g", FunType(rec, age)),
      AttrType("_3", FunType(rec2, BoolType()))))))
  }

  test("""let-polymorphism #6""") {
    val z = TypeVariable()
    val n = NumberType()
    success(
      """
        {
        x := set(1);
        f := \v -> for (z <- x; z = v) yield set z;

        v := f(10);
        f
        }

      """, TestWorlds.empty,
      FunType(IntType(), CollectionType(SetMonoid(),IntType())))
  }

  test("""let-polymorphism #7""") {
    val n1 = NumberType()
    val n2 = NumberType()
    success(
      """
      {
        f := \x -> {
          f1 := (\x1 -> (x1 = (x + x)));
          f2 := (\x2 -> (x2 != (x + x)));
          \z -> ((if true then f1 else f2)(z))
        };
        (f, f(1))
      }
      """, TestWorlds.empty,
      RecordType(Attributes(List(AttrType("f", FunType(n1, FunType(n1, BoolType()))), AttrType("_2", FunType(n2, BoolType()))))))
  }

  test("map") {
    success(
      """
        {
        map := \(col, f) -> for (el <- col) yield list f(el);
        col1 := list(1);
        col2 := list(1.0);
        (map(col1, \x -> x + 1), map(col2, \x -> x + 1.1))
        }
      """, TestWorlds.empty, RecordType(Attributes(List(AttrType("_1", CollectionType(ListMonoid(),IntType())), AttrType("_2", CollectionType(ListMonoid(),FloatType()))))))
  }

  test("""\(x, y) -> x.age = y""") {
    val y = TypeVariable()
    success("""\(x, y) -> x.age = y""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(RecordType(AttributesVariable(Set(AttrType("age", y))))), PatternAttrType(y))), BoolType()))
  }

  test("""\(x, y) -> (x, y)""") {
    val x = TypeVariable()
    val y = TypeVariable()
    success("""\(x, y) -> (x, y)""", TestWorlds.empty, FunType(PatternType(List(PatternAttrType(x), PatternAttrType(y))), RecordType(Attributes(List(AttrType("x", x), AttrType("y", y))))))
  }

  test("""\(x,y) -> for (z <- x) yield sum y(z)""") {
    val m = GenericMonoid(commutative = None, idempotent = Some(false))
    val z = TypeVariable()
    val yz = NumberType()
    success("""\(x,y) -> for (z <- x) yield sum y(z)""", TestWorlds.empty,
      FunType(
        PatternType(List(PatternAttrType(CollectionType(m, z)), PatternAttrType(FunType(z, yz)))),
        yz))
  }

  test("""\(x,y) -> for (z <- x) yield max y(z)""") {
    val m = GenericMonoid(commutative = None, idempotent = None)
    val z = TypeVariable()
    val yz = NumberType()
    success("""\(x,y) -> for (z <- x) yield max y(z)""", TestWorlds.empty,
      FunType(
        PatternType(List(PatternAttrType(CollectionType(m, z)), PatternAttrType(FunType(z, yz)))),
        yz))
  }

  test("""(\x -> x + 1)(1)""") {
    success( """(\x -> x + 1)(1)""", TestWorlds.empty, IntType())
  }

  test("""(\y -> (\x -> x + 1)(y))(1)""") {
    success("""(\y -> (\x -> x + 1)(y))(1)""", TestWorlds.empty, IntType())
  }

  test("recursive lambda #1") {
    success(
      """
         {
         fact1 := \(f, n1) -> if (n1 = 0) then 1 else n1 * (f(f, n1 - 1));
         fact := \n -> fact1(fact1, n);
         fact
        }
      """, TestWorlds.empty, FunType(IntType(), IntType()))
  }

  test("recursive lambda #2") {
    success(
      """
      {
        F := \f -> (\x -> f(f,x));
        fact1 := \(f, n1) -> if (n1 = 0) then 1 else n1 * (f(f, n1 - 1));
        fact := F(fact1);
        fact
      }
      """, TestWorlds.empty, FunType(IntType(), IntType())
    )

  }

  {
    val oint = IntType()
    oint.nullable = true
    val ob = BoolType()
    ob.nullable = true
    val oset = CollectionType(SetMonoid(), BoolType())
    oset.nullable = true
    test("LI") {
      success("LI", TestWorlds.options, CollectionType(ListMonoid(), IntType()))
    }
    test("LOI") {
      success("LOI", TestWorlds.options, CollectionType(ListMonoid(), oint))
    }
    test("for (i <- LI) yield set i") {
      success("for (i <- LI) yield set i", TestWorlds.options, CollectionType(SetMonoid(), IntType()))
    }
    test("for (i <- LI) yield set 1+i") {
      success("for (i <- LI) yield set 1+i", TestWorlds.options, CollectionType(SetMonoid(), IntType()))
    }
    test("for (i <- LOI) yield set i") {
      success("for (i <- LOI) yield set i", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("for (i <- LOI) yield set 1+i") {
      success("for (i <- LOI) yield set 1+i", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("for (i <-LI; oi <- LOI) yield set oi+i") {
      success("for (i <-LI; oi <- LOI) yield set oi+i", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("(for (i <- LI) yield set i) union (for (i <- LI) yield set i)") {
      success("(for (i <- LI) yield set i) union (for (i <- LI) yield set i)", TestWorlds.options, CollectionType(SetMonoid(), IntType()))
    }
    test("(for (i <- LOI) yield set i) union (for (i <- LOI) yield set i)") {
      success("(for (i <- LOI) yield set i) union (for (i <- LOI) yield set i)", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("(for (i <- LOI) yield set i) union (for (i <- LI) yield set i)") {
      success("(for (i <- LOI) yield set i) union (for (i <- LI) yield set i)", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("(for (i <- LI) yield set i) union (for (i <- LOI) yield set i)") {
      success("(for (i <- LI) yield set i) union (for (i <- LOI) yield set i)", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("for (i <- LI) yield max i") {
      success("for (i <- LI) yield max i", TestWorlds.options, IntType())
    }
    test("for (i <- LOI) yield max i") {
      success("for (i <- LOI) yield max i", TestWorlds.options, IntType())
    }
    test("for (i <- OLI) yield max i") {
      success("for (i <- OLI) yield max i", TestWorlds.options, oint)
    }
    test("for (i <- OLOI) yield max i") {
      success("for (i <- OLOI) yield max i", TestWorlds.options, oint)
    }
    test("for (i <- LI) yield list i") {
      success("for (i <- LI) yield list i", TestWorlds.options, CollectionType(ListMonoid(), IntType()))
    }
    test("for (i <- LOI) yield list i") {
      success("for (i <- LOI) yield list i", TestWorlds.options, CollectionType(ListMonoid(), oint))
    }
    test("for (i <- OLI) yield list i") {
      success("for (i <- OLI) yield list i", TestWorlds.options, {
        val ot = CollectionType(ListMonoid(), IntType()); ot.nullable = true; ot
      })
    }
    test("for (i <- OLOI) yield list i") {
      success("for (i <- OLOI) yield list i", TestWorlds.options, {
        val ot = CollectionType(ListMonoid(), oint); ot.nullable = true; ot
      })
    }
    test("{ m := for (i <- LOI) yield max i; for (i <- LI; i < m) yield set i }") {
      success("{ m := for (i <- LOI) yield max i; for (i <- LI; i < m) yield set i }", TestWorlds.options, CollectionType(SetMonoid(), IntType()))
    }
    test("for (i <- LOI; i < 1) yield set i") {
      success("for (i <- LOI; i < 1) yield set i", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("{ m := for (i <- LOI) yield max i; for (i <- LOI; i < m) yield set i }") {
      success("{ m := for (i <- LOI) yield max i; for (i <- LOI; i < m) yield set i }", TestWorlds.options, CollectionType(SetMonoid(), oint))
    }
    test("for (r <- records) yield set (r.OI > 10)") {
      success("for (r <- records) yield set (r.OI > 10)", TestWorlds.options, CollectionType(SetMonoid(), ob))
    }
    test("for (r <- records) yield set (r.I > 10)") {
      success("for (r <- records) yield set (r.I > 10)", TestWorlds.options, CollectionType(SetMonoid(), BoolType()))
    }
    test("for (i <- OLI) yield set (i > 10)") {
      success("for (i <- OLI) yield set (i > 10)", TestWorlds.options, oset)
    }
    test("""{ f := \x -> x > 10 ; for (i <- LI) yield set f(i) } """) {
      success("""{ f := \x -> x > 10 ; for (i <- LI) yield set f(i) } """, TestWorlds.options, CollectionType(SetMonoid(), BoolType()))
    }
    test("""{ f := \x -> x > 10 ; for (i <- LOI) yield set f(i) } """) {
      success("""{ f := \x -> x > 10 ; for (i <- LOI) yield set f(i) } """, TestWorlds.options, CollectionType(SetMonoid(), ob))
    }
    test("""{ f := \x -> x > 10 ; for (i <- OLI) yield set f(i) } """) {
      success("""{ f := \x -> x > 10 ; for (i <- OLI) yield set f(i) } """, TestWorlds.options, {
        val ot = CollectionType(SetMonoid(), BoolType()); ot.nullable = true; ot
      })
    }
    test("""{ f := \x -> x > 10 ; for (i <- OLOI) yield set f(i) } """) {
      success("""{ f := \x -> x > 10 ; for (i <- OLOI) yield set f(i) } """, TestWorlds.options, {
        val ot = CollectionType(SetMonoid(), ob); ot.nullable = true; ot
      })
    }
    test("""for (i <- LI) yield set (\x -> x < i)""") {
      success("""for (i <- LI) yield set (\x -> x < i)""", TestWorlds.options, CollectionType(SetMonoid(), FunType(IntType(), BoolType())))
    }
    test("""for (i <- LOI) yield set (\x -> x < i)""") {
      success("""for (i <- LOI) yield set (\x -> x < i)""", TestWorlds.options, CollectionType(SetMonoid(), FunType(IntType(), ob)))
    }
    test("""for (i <- OLI) yield set (\x -> x < i)""") {
      success("""for (i <- OLI) yield set (\x -> x < i)""", TestWorlds.options, {
        val ot = CollectionType(SetMonoid(), FunType(IntType(), BoolType())); ot.nullable = true; ot
      })
    }
    test("""for (i <- OLOI) yield set (\x -> x < i)""") {
      success("""for (i <- OLOI) yield set (\x -> x < i)""", TestWorlds.options, {
        val ot = CollectionType(SetMonoid(), FunType(IntType(), ob)); ot.nullable = true; ot
      })
    }

    test("""for (i <- OLOI) yield set (\(x, (y, z)) -> x < i and (y+z) > i)""") {
      success("""for (i <- OLOI) yield set (\(x, (y, z)) -> x < i and (y+z) > i)""", TestWorlds.options, {
        val ot = CollectionType(SetMonoid(), FunType(PatternType(List(PatternAttrType(IntType()),
          PatternAttrType(RecordType(Attributes(List(
            AttrType("_1", IntType()), AttrType("_2", IntType()))))))), ob))
        ot.nullable = true
        ot
      })
    }
  }

  ignore("options in select") {
    success("select s from s in LI", TestWorlds.options, CollectionType(BagMonoid(), IntType()))
    success("select partition from s in LI group by s", TestWorlds.options, CollectionType(BagMonoid(), CollectionType(BagMonoid(), IntType())))

    {
      // we check the type of "partition" when it comes from an option bag of integers. Itself should be an option bag of integers, wrapped into
      // an option bag because the select itself inherits the option of its sources.
      val partitionType = CollectionType(BagMonoid(), IntType());
      partitionType.nullable = true // because OLI is an option list
      val selectType = CollectionType(BagMonoid(), partitionType);
      selectType.nullable = true // because OLI is an option list
      success("select partition from s in OLI group by s", TestWorlds.options, selectType)
    }
  }

  ignore("fancy select with options") {
      // more fancy. We join a bag of option(int) with an option bag(int).
      // partition should be an option bag of record with an option int and a non-option int.
      val optionInt = IntType()
      optionInt.nullable = true
      val partitionType = CollectionType(BagMonoid(), RecordType(Attributes(List(AttrType("_1", optionInt), AttrType("_2", IntType())))))
      partitionType.nullable = true // because OLI is an option list
      // select should be an option bag of ...
      val selectType = CollectionType(BagMonoid(), RecordType(Attributes(List(AttrType("_1", optionInt), AttrType("_2", partitionType)))));
      selectType.nullable = true // because OLI is an option list
      success("select s+r, partition from s in OLI, r in LOI where s = r group by s+r", TestWorlds.options, selectType)
    }

  test("select s from s in students") {
    success("select s from s in students", TestWorlds.professors_students, CollectionType(ListMonoid(), UserType(Symbol("student"))))
  }

  test("select distinct s from s in students") {
    success("select distinct s from s in students", TestWorlds.professors_students, CollectionType(SetMonoid(), UserType(Symbol("student"))))
  }

  test("select distinct s.age from s in students") {
    success("select distinct s.age from s in students", TestWorlds.professors_students, CollectionType(SetMonoid(), IntType()))
  }

  test("select s.age from s in students order by s.age") {
    success("select s.age from s in students order by s.age", TestWorlds.professors_students, CollectionType(ListMonoid(), IntType()))
  }

  test("select s.lastname from s in (select s.name as lastname from s in students)") {
    success("""select s.lastname from s in (select s.name as lastname from s in students)""", TestWorlds.professors_students, CollectionType(ListMonoid(), StringType()))
  }

  ignore("wrong field name") {
    failure("""select s.astname from s in (select s.name as lastname from s in students)""", TestWorlds.professors_students, ???)
  }

  test("partition") {
    failure("partition", TestWorlds.empty, UnknownPartition(Calculus.Partition()))
  }

  test("select partition from students s") {
    failure("select partition from students s", TestWorlds.professors_students, UnknownPartition(Calculus.Partition()))
  }

  test("select s.age, partition from students s group by s.age") {
    success("select s.age, partition from students s group by s.age", TestWorlds.professors_students, CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("partition", UserType(Symbol("students"))))))))
  }

  test("select partition from students s group by s.age") {
    success("select partition from students s group by s.age", TestWorlds.professors_students, CollectionType(ListMonoid(), UserType(Symbol("students"))))
  }

  test("select s.age, (select p.name from partition p) from students s group by s.age") {
    success("select s.age, (select p.name from partition p) as names from students s group by s.age", TestWorlds.professors_students, CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("names", CollectionType(ListMonoid(), StringType())))))))
  }

  test("select s.dept, count(partition) as n from students s group by s.dept") {
    success("select s.department, count(partition) as n from students s group by s.department", TestWorlds.school, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("department", StringType()), AttrType("n", IntType()))))))
  }

  ignore("select dpt, count(partition) as n from students s group by dpt: s.dept") {
    success("select dpt, count(partition) as n from students s group by dpt: s.dept", TestWorlds.professors_students, ???)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    success("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10", TestWorlds.professors_students, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("decade",IntType()), AttrType("names",CollectionType(ListMonoid(),StringType())))))))
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    success("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age", TestWorlds.professors_students, CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("age",IntType()), AttrType("names",CollectionType(ListMonoid(),RecordType(Attributes(List(AttrType("name",StringType()), AttrType("partition",UserType(Symbol("students")))))))))))))
  }

  test("sum(list(1))") {
    success("sum(list(1))", TestWorlds.empty, IntType())
  }

  test("sum(list(1.1))") {
    success("sum(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("sum(1)") {
    failure("sum(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(GenericMonoid(idempotent=Some(false)), NumberType())))
  }

  test("max(list(1))") {
    success("max(list(1))", TestWorlds.empty, IntType())
  }

  test("max(list(1.1))") {
    success("max(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("max(1)") {
    failure("max(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(GenericMonoid(), NumberType())))
  }

  test("min(list(1))") {
    success("min(list(1))", TestWorlds.empty, IntType())
  }

  test("min(list(1.1))") {
    success("min(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("min(1)") {
    failure("min(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(GenericMonoid(), NumberType())))
  }

  test("avg(list(1))") {
    success("avg(list(1))", TestWorlds.empty, IntType())
  }

  test("avg(list(1.1))") {
    success("avg(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("avg(1)") {
    failure("avg(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(GenericMonoid(), NumberType())))
  }

  test("count(list(1))") {
    success("count(list(1))", TestWorlds.empty, IntType())
  }

  test("count(list(1.1))") {
    success("count(list(1.1))", TestWorlds.empty, IntType())
  }

  test("""count(list("foo"))""") {
    success("""count(list("foo"))""", TestWorlds.empty, IntType())
  }

  test("""count(1)""") {
    failure("count(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(GenericMonoid(), TypeVariable())))
  }

  test("to_bag(list(1))") {
    success("""to_bag(list(1))""", TestWorlds.empty, CollectionType(BagMonoid(), IntType()))
  }

  test("to_set(list(1))") {
    success("""to_set(list(1))""", TestWorlds.empty, CollectionType(SetMonoid(), IntType()))
  }

  test("to_list(set(1))") {
    success("""to_list(set(1))""", TestWorlds.empty, CollectionType(ListMonoid(), IntType()))
  }

  test("1 in list(1)") {
    success("1 in list(1)", TestWorlds.empty, BoolType())
  }

  test("1 in list(true)") {
    failure("1 in list(true)", TestWorlds.empty, UnexpectedType(IntType(), BoolType()))
  }

  test("\\x -> x in list(1)") {
    success("{ f := \\x -> x in list(1); for (x <- unknown ; f(x)) yield set x }", TestWorlds.unknown, CollectionType(SetMonoid(), IntType()))
  }

  test("exists(list(1))") {
    success("exists(list(1))", TestWorlds.empty, BoolType())
  }

  test("polymorphic select #1") {
    success(
      """
        |{
        |  a := \xs -> select x.age from x in xs;
        |  (a(students), a(professors))
        |}
      """.stripMargin, TestWorlds.professors_students, IntType())
  }

  test("poloymorphic select #2") {
    //TODO: Triple check that this parses well!
    success("""\x,y -> for (a <- x; b <- y) yield a > max(x) and b > count(y)""", TestWorlds.empty, AnyType())
  }

  test("polymorphic partition") {
    success(
      """
        |{
        |  a := \xs -> select x.age, count(partition) from x in xs where x.age > 15;
        |  (a(students), a(professors))
        |}
      """.stripMargin, TestWorlds.professors_students, IntType())
  }

  test("polymorphic partition #2") {
    success(
      """
        |{
        |  a := \xs -> select x.age, partition from x in xs where x.age > 15;
        |  (a(students), a(professors))
        |}
      """.stripMargin, TestWorlds.professors_students, IntType())
  }

  test("polymorphic partial record w/ polymorphic select") {
    success(
      """
        |{
        |  global_func := \xs -> select x.age from x in xs;
        |  a := \x -> x.age1 > x.age2 && x.func == global_func;
        |  (
        |    a,
        |    a((age1: 10, age2: 20, func(students)),
        |    a((age1: 15, age2: 25, func(professors))
        |  )
        |}
      """.stripMargin, TestWorlds.empty, IntType())
  }

  test("for ( <- students ) yield set name") {
    success("for ( <- students ) yield set name", TestWorlds.professors_students, CollectionType(SetMonoid(), StringType()))
  }

  test("for ( <- students ) yield set (name, age)") {
    success("for ( <- students ) yield set (name, age)", TestWorlds.professors_students, CollectionType(SetMonoid(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))
  }

  test("for ( <- students; <- professors ) yield set name") {
    failure("for ( <- students; <- professors ) yield set name", TestWorlds.professors_students, AmbiguousIdn(IdnUse("name")))
  }

  test("for ( <- students; age > (for ( <- professors ) yield max age)) yield set (name, age)") {
    success("for ( <- students ) yield set (name, age)", TestWorlds.professors_students, CollectionType(SetMonoid(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))
  }

  test("(a: 1, a: 2)") {
    failure("(a: 1, a: 2)", TestWorlds.empty, AmbiguousIdn(IdnUse("a")))
  }

  test("{ a := 1; (a, a) }") {
    failure("{ a := 1; (a, a) }", TestWorlds.empty, AmbiguousIdn(IdnUse("a")))
  }

  test("simple function call") {
    success(
      """
        |{
        | a := \(x,y) -> x + y;
        |
        | i := 1;
        | j := 2;
        | a(i,j)
        |}
      """.stripMargin, TestWorlds.empty, IntType())
  }

  test("simple function call #2") {
    success(
      """
        |{
        | a := \(x,y) -> x + y;
        |
        | a(i: 1,j: 2)
        |}
        |""".stripMargin, TestWorlds.empty, IntType())
  }

  test("extract generator") {
    success("""for ((name, age) <- students) yield set name""", TestWorlds.professors_students, CollectionType(SetMonoid(), StringType()))
  }

  test("extract bad generator") {
    failure("""for ((name, age, foo) <- students) yield set name""", TestWorlds.professors_students, PatternMismatch(PatternProd(List(PatternIdn(IdnDef("name")), PatternIdn(IdnDef("age")), PatternIdn(IdnDef("foo")))), UserType(Symbol("student"))))
  }

  test("cucu simpler") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    success(
      """
        |{
        | group_by_age := \xs -> for (x <- xs) yield list (x.age, (for (x1 <- xs; x1.age = x.age) yield list x1));
        | group_by_age(students)
        |}
      """.stripMargin,

      TestWorlds.professors_students,
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(AttrType("age", IntType()),
                                   AttrType("_2", CollectionType(ListMonoid(),
                                                                 RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))

  }

  test("...") {
    success(
      """
        |{
        | group_by_age := \xs -> for (x <- xs) yield list x;
        | group_by_age(students)
        |}
      """.stripMargin,

      TestWorlds.professors_students,
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))

  }

  test("cucu simpler #2") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    success(
      """
        |{
        | group_by_age := \xs -> for (x <- xs) yield list (x.age, (for (x1 <- xs; x1.age = x.age) yield list x1));
        | (group_by_age(students), group_by_age(professors))
        |}
      """.stripMargin,

      TestWorlds.professors_students,
      RecordType(Attributes(List(AttrType("_1",
        CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("_2", CollectionType(ListMonoid(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))))))),
        AttrType("_2",
          CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()),
            AttrType("_2", CollectionType(ListMonoid(),
              RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))))))

  }

  test("group_by_age(students), select version") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    success(
      """
        |{
        | group_by_age := \xs -> select x.age, partition from x in xs group by x.age;
        | group_by_age(students)
        |}
      """.stripMargin,

      TestWorlds.professors_students,
      CollectionType(ListMonoid(),
        RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(ListMonoid(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))

  }

  test("""\xs -> select x from x in xs (monoid should remain)""") {
    val m = MonoidVariable()
    val t = TypeVariable()
    success("""\xs -> select x from x in xs""", TestWorlds.empty, FunType(CollectionType(m, t), CollectionType(m, t)))
  }

  test("select x from x in xs (applied to students)") {
    val m = MonoidVariable()
    val t = TypeVariable()
    success("""{
       a := \xs -> select x from x in xs;
       a(students)
       } """, TestWorlds.professors_students,       CollectionType(ListMonoid(),
          RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))

  }

  test("polymorphic select") {
    success(
      """
        |{
        | group_by_age := \xs -> select x.age, partition from x in xs group by x.age;
        | (group_by_age(students), group_by_age(professors))
        |}
      """.stripMargin,

      TestWorlds.professors_students,
      RecordType(Attributes(List(AttrType("_1",
        CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(ListMonoid(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))))))),
                                 AttrType("_2",
        CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(ListMonoid(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))))))
  }

  test("select * from students") {
    success(
      "select * from students",
      TestWorlds.professors_students,
      UserType(Symbol("students")))
  }

  test("select age, * from students") {
    success(
      "select age, * from students",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", UserType(Symbol("students"))))))))
  }

  test("select age, * from students group by age") {
    success(
      "select age, * from students group by age",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", UserType(Symbol("students"))))))))
  }

  test("select age, count(*) from students") {
    success(
      "select age, count(*) from students",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", IntType()))))))
  }

  test("select age, count(*) from students group by age") {
    success(
      "select age, count(*) from students group by age",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", IntType()))))))
  }

  test("select age, (select name from partition), count(*), max(select age from partition) from students group by age") {
    success(
      "select age, (select name from partition), count(*), max(select age from partition) from students group by age",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", CollectionType(ListMonoid(), StringType())), AttrType("_3", IntType()), AttrType("_4", IntType()))))))
  }

  test("select name, count(*) from students group by age") {
    success(
      "select name, count(*) from students group by age",
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("_2", IntType()))))))
  }

  test("for ( <- students) yield list *") {
    success(
      "for ( <- students) yield list *",
      TestWorlds.professors_students,
      UserType(Symbol("students")))
  }

  test("list(1,2,3)") {
    success("list(1, 2, 3)", TestWorlds.empty, IntType())
  }

  test("""set(("dbname", "authors"), ("dbname", "publications"))""") {
    success(
      """set(("dbname", "authors"), ("dbname", "publications"))""",
      TestWorlds.empty,
      IntType())
  }

  test("list over comprehension") {
    val t = TypeVariable()
    success(
      """\xs -> for (x <- xs) yield list x""",
      TestWorlds.empty,
      FunType(
        CollectionType(GenericMonoid(commutative=Some(false), idempotent=Some(false)), t),
        CollectionType(ListMonoid(), t)))
  }

  test("list over polymorphic comprehension") {
    val t = TypeVariable()
    success(
      """
        |{
        | a := \xs -> for (x <- xs) yield list x;
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        CollectionType(GenericMonoid(commutative=Some(false), idempotent=Some(false)), t),
        CollectionType(ListMonoid(), t)))
  }

  test("2x list over polymorphic comprehension") {
    val t1 = TypeVariable()
    val t2 = TypeVariable()
    success(
      """
        |{
        | a := \xs -> for (x <- xs) yield list x;
        | (field1: a, field2: a)
        |}
      """.stripMargin,
      TestWorlds.empty,
      RecordType(Attributes(List(
        AttrType("field1", FunType(
          CollectionType(GenericMonoid(commutative=Some(false), idempotent=Some(false)), t1),
          CollectionType(ListMonoid(), t1))),
        AttrType("field2", FunType(
          CollectionType(GenericMonoid(commutative=Some(false), idempotent=Some(false)), t2),
          CollectionType(ListMonoid(), t2)))))))
  }

  test("sum over polymorphic comprehension #1") {
    val n = NumberType()
    success(
      """
        |{
        | a := \xs -> for (x <- xs) yield sum x;
        | a
        |}
      """.stripMargin,
    TestWorlds.empty,
    FunType(CollectionType(GenericMonoid(idempotent=Some(false)), n), n))
  }

  test("sum over polymorphic comprehension #2") {
    val n = NumberType()
    success(
      """
        |{
        | a := \xs,ys -> for (x <- xs; y <- ys) yield sum x;
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        PatternType(List(
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), n)),
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), TypeVariable())))),
        n))
  }

  test("bag over polymorphic comprehension") {
    val t = TypeVariable()
    success(
      """
        |{
        | a := \xs, ys -> for (i <- (select x from x in xs, y in ys)) yield bag i;
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        PatternType(List(
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), t)),
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), TypeVariable())))),
        CollectionType(BagMonoid(), t))
    )
  }


  test("sum over polymorphic select") {
    val n = NumberType()
    success(
      """
        |{
        | a := \xs, ys -> sum(select x from x in xs, y in ys);
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        PatternType(List(
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), n)),
          PatternAttrType(CollectionType(GenericMonoid(idempotent=Some(false)), TypeVariable())))),
        n))
  }

  test("select x from x in students, y in professors") {
    success(
      """select x from x in students, y in professors""",
      TestWorlds.professors_students,
      IntType())
  }

  test("\\xs -> sum(select x.age from x in students, y in xs)") {
    success(
      """\xs -> sum(select x.age from x in students, y in xs)""",
      TestWorlds.professors_students,
      FunType(CollectionType(GenericMonoid(idempotent=Some(false)), TypeVariable()), IntType()))
  }

  test("build 2d array and scan it") {
    success(
      """
        |{
        |  a := list((1,2,3),(4,5,6));
        |  for ( row <- a )
        |    yield list row
        |}
      """.stripMargin,
      TestWorlds.empty,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("_2", IntType()), AttrType("_3", IntType()))))))

  }
  test("list((1,2,3),(4,5,6))") {
    success(
      """list((1,2,3),(4,5,6))""",
      TestWorlds.empty,
      CollectionType(ListMonoid(), RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("_2", IntType()), AttrType("_3", IntType()))))))
  }

}



