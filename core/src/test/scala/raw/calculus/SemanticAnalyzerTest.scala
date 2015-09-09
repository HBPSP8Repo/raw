package raw
package calculus

import scala.collection.immutable.Seq

class SemanticAnalyzerTest extends FunTest {

  import raw.calculus.Calculus.{IdnDef, IdnUse}

  def go(query: String, world: World) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)
    logger.debug(s"AST: ${t.root}")
    logger.debug(s"Parsed tree: ${CalculusPrettyPrinter(t.root)}")

    val analyzer = new SemanticAnalyzer(t, world, Some(query))
    analyzer.errors.foreach(e => logger.error(ErrorsPrettyPrinter(e)))
    analyzer.printTypedTree()
    analyzer
  }

  def success(query: String, world: World, expectedType: Type) = {
    val analyzer = go(query, world)
    val inferredType = analyzer.tipe(analyzer.tree.root)
    assert(analyzer.errors.isEmpty)
    analyzer.printTypedTree()
    logger.debug(s"Actual type: ${TypesPrettyPrinter(inferredType)}")
    logger.debug(s"Expected type: ${TypesPrettyPrinter(expectedType)}")
    compare(inferredType.toString, expectedType.toString)
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
        assert(analyzer.errors.exists {
          case IncompatibleTypes(`t1`, `t2`) => true
          case IncompatibleTypes(`t2`, `t1`) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      // Ignore text description in expected types, even if defined
      case UnexpectedType(t, expected, _) =>
        assert(analyzer.errors.exists {
          case UnexpectedType(`t`, `expected`, _) => true
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

  test("for (e <- Events) yield set e") {
    success("for (e <- Events) yield set e", TestWorlds.cern, SetType(TestWorlds.cern.sources("Events").asInstanceOf[ListType].innerType))
  }

  test("for (e <- Events; m <- e.muons) yield set m") {
    success("for (e <- Events; m <- e.muons) yield set m", TestWorlds.cern, SetType(RecordType(List(AttrType("pt", FloatType()), AttrType("eta", FloatType())), None)))
  }

  test("for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)") {
    success("for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)", TestWorlds.cern, SetType(RecordType(List(AttrType("muon", RecordType(List(AttrType("pt", FloatType()), AttrType("eta", FloatType())), None))), None)))
  }

  test("for (i <- Items) yield set i") {
    success("for (i <- Items) yield set i", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
  }

  test("for (i <- Items) yield set i.next") {
    success("for (i <- Items) yield set i.next", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
  }

  test("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x") {
    success("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x", TestWorlds.linkedList, SetType(UserType(Symbol("Item"))))
  }

  test("departments - from Fegaras's paper") {
    success(
      """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""", TestWorlds.departments,
      SetType(RecordType(List(AttrType("name", StringType()), AttrType("address", RecordType(List(AttrType("street", StringType()), AttrType("zipcode", StringType())), None))), None)))
  }

  test("for (d <- Departments) yield set d") {
    success( """for (d <- Departments) yield set d""", TestWorlds.departments, SetType(UserType(Symbol("Department"))))
  }

  test( """for ( d <- Departments; d.name = "CSE") yield set d""") {
    success( """for ( d <- Departments; d.name = "CSE") yield set d""", TestWorlds.departments, SetType(UserType(Symbol("Department"))))
  }

  test( """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""") {
    success( """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""", TestWorlds.departments, SetType(RecordType(List(AttrType("deptName", StringType())), None)))
  }

  test("employees - from Fegaras's paper") {
    success(
      "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)", TestWorlds.employees,
      SetType(RecordType(List(AttrType("E", RecordType(List(AttrType("dno", IntType()), AttrType("children", ListType(RecordType(List(AttrType("age", IntType())), None))), AttrType("manager", RecordType(List(AttrType("name", StringType()), AttrType("children", ListType(RecordType(List(AttrType("age", IntType())), None)))), None))), None)), AttrType("M", IntType())), None)))
  }

  test("for (r <- integers) yield max r") {
    success("for (r <- integers) yield max r", TestWorlds.simple, IntType())
  }

  test("for (r <- integers) yield set r + 1") {
    success("for (r <- integers) yield set r + 1", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown) yield set r + 1") {
    success("for (r <- unknown) yield set r + 1", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown) yield set r + 1.0") {
    success("for (r <- unknown) yield set r + 1.0", TestWorlds.simple, SetType(FloatType()))
  }

  test("for (r <- unknown; x <- integers; r = x) yield set r") {
    success("for (r <- unknown; x <- integers; r = x) yield set r", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown; x <- integers; r + x = 2*x) yield set r") {
    success("for (r <- unknown; x <- integers; r + x = 2*x) yield set r", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown; x <- floats; r + x = x) yield set r") {
    success("for (r <- unknown; x <- floats; r + x = x) yield set r", TestWorlds.simple, SetType(FloatType()))
  }

  test("for (r <- unknown; x <- booleans; r and x) yield set r") {
    success("for (r <- unknown; x <- booleans; r and x) yield set r", TestWorlds.simple, SetType(BoolType()))
  }

  test("for (r <- unknown; x <- strings; r = x) yield set r") {
    success("for (r <- unknown; x <- strings; r = x) yield set r", TestWorlds.simple, SetType(StringType()))
  }

  test("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))") {
    success("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))", TestWorlds.simple, IntType())
  }

  test("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r") {
    success("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r", TestWorlds.simple, SetType(IntType()))
  }

  test( """for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""") {
    success( """for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown; v := r) yield set (r + 0)") {
    success("for (r <- unknown; v := r) yield set (r + 0)", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)") {
    success("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)", TestWorlds.simple, SetType(IntType()))
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
    success("{ x := for (i <- integers) yield set i; x }", TestWorlds.simple, SetType(IntType()))
  }

  test("{ x := for (i <- integers) yield set i; for (y <- x) yield set y }") {
    success("{ x := for (i <- integers) yield set i; for (y <- x) yield set y }", TestWorlds.simple, SetType(IntType()))
  }

  test("{ z := 42; x := for (i <- integers) yield set i; for (y <- x) yield set y }") {
    success("{ z := 42; x := for (i <- integers) yield set i; for (y <- x) yield set y }", TestWorlds.simple, SetType(IntType()))
  }

  test("{ z := 42; x := for (i <- integers; i = z) yield set i; for (y <- x) yield set y }") {
    success("{ z := 42; x := for (i <- integers; i = z) yield set i; for (y <- x) yield set y }", TestWorlds.simple, SetType(IntType()))
  }

  test("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r") {
    success("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", TestWorlds.unknown, SetType(ConstraintRecordType(Set(AttrType("age", IntType()), AttrType("birth", IntType()), AttrType("alive", BoolType())))))
  }

  test("for (r <- unknownrecords) yield set r.dead or r.alive") {
    success("for (r <- unknownrecords) yield set r.dead or r.alive", TestWorlds.unknown, SetType(BoolType()))
  }

  test("for (r <- unknownrecords; r.dead or r.alive) yield set r") {
    success("for (r <- unknownrecords; r.dead or r.alive) yield set r", TestWorlds.unknown, SetType(RecordType(List(AttrType("dead", BoolType()), AttrType("alive", BoolType())), None)))
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
    """, TestWorlds.simple, SetType(IntType()))
  }

  test("""\a -> a + 2""") {
    success( """\a -> a + 2""", TestWorlds.empty, FunType(IntType(), IntType()))
  }

  test( """\a -> a + a + 2""") {
    success( """\a -> a + a + 2""", TestWorlds.empty, FunType(IntType(), IntType()))
  }

  test( """\(a, b) -> a + b + 2""") {
    success( """\(a, b) -> a + b + 2""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
  }

  test( """\a -> a""") {
    val a = TypeVariable()
    success( """\a -> a""", TestWorlds.empty, FunType(a, a))
  }

  test( """\x -> x.age + 2""") {
    success( """\x -> x.age + 2""", TestWorlds.empty, FunType(ConstraintRecordType(Set(AttrType("age", IntType()))), IntType()))
  }

  test( """\(x, y) -> x + y""") {
    val n = NumberType()
    success( """\(x, y) -> x + y""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", n), AttrType("_2", n)), None), n))
  }

  test("""{ recursive := \(f, arg) -> f(arg); recursive } """) {
    var arg = TypeVariable()
    val out = TypeVariable()
    val f = FunType(arg, out)
    success( """{ recursive := \(f, arg) -> f(arg); recursive } """, TestWorlds.empty, FunType(RecordType(List(AttrType("_1", f), AttrType("_2", arg)), None), out))
  }

//     TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//    success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
//      FunType(
//        ConstraintCollectionType(ConstraintRecordType(Set(AttrType("age", IntType()), AttrType("name", TypeVariable()))), None, None),
//        BagType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", TypeVariable())), None))))

  test("for ((a, b) <- list((1, 2.2))) yield set (a, b)") {
    success("""for ((a, b) <- list((1, 2.2))) yield set (a, b)""", TestWorlds.empty, SetType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", FloatType())), None)))
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
    failure("if 1 then 1 else 0", TestWorlds.empty, UnexpectedType(IntType(), BoolType(), None))
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

  test("for (s <- students) yield list s") {
    success("for (s <- students) yield list s", TestWorlds.professors_students, ListType(UserType(Symbol("student"))))
  }

  test("for (s <- students) yield list (a := 1, b := s)") {
    success("for (s <- students) yield list (a := 1, b := s)", TestWorlds.professors_students, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", UserType(Symbol("student")))), None)))
  }

  test("for (s <- students; p <- professors; s = p) yield list s") {
    failure("for (s <- students; p <- professors; s = p) yield list s", TestWorlds.professors_students, IncompatibleTypes(UserType(Symbol("professor")), UserType(Symbol("student"))))
  }

  test("for (s <- students; p <- professors) yield list (name := s.name, age := p.age)") {
    success("for (s <- students; p <- professors) yield list (name := s.name, age := p.age)", TestWorlds.professors_students, ListType(RecordType(List(AttrType("name", StringType()), AttrType("age", IntType())), None)))
  }

  test("for (s <- students; p <- professors) yield list (a := 1, b := s, c := p)") {
    success("for (s <- students; p <- professors) yield list (a := 1, b := s, c := p)", TestWorlds.professors_students, ListType(RecordType(List(AttrType("a", IntType()), AttrType("b", UserType(Symbol("student"))), AttrType("c", UserType(Symbol("professor")))), None)))
  }

  test("""\(x, y) -> x + y + 10""") {
    success("""\(x, y) -> x + y + 10""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", IntType()), AttrType("_2", IntType())), None), IntType()))
  }

  test("""\(x, y) -> x + y + 10.2""") {
    success("""\(x, y) -> x + y + 10.2""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", FloatType()), AttrType("_2", FloatType())), None), FloatType()))
  }

  test("""\(x, y) -> { z := x; y + z }""") {
    val n = NumberType()
    success("""\(x, y) -> { z := x; y + z }""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", n), AttrType("_2", n)), None), n))
  }

  test("""{ x := { y := 1; z := y; z }; x }""") {
    success("""{ x := { y := 1; z := y; z }; x }""", TestWorlds.empty, IntType())
  }

  test("""let polymorphism - not binding into functions""") {
    val z = TypeVariable()
    val n = NumberType()
    success(
      """
        {
        sum1 := \(x,y) -> for (z <- x) yield sum y(z);
        age := (students, \x -> x.age);
        v := sum1(age);
        sum1
        }

      """, TestWorlds.professors_students,
      FunType(RecordType(List(AttrType("_1", ConstraintCollectionType(z, None, None)), AttrType("_2", FunType(z, n))), None), n))
  }

  test("""let-polymorphism #1""") {
    success(
      """
        {
          f := \x -> x;
          (f(1), f(true))
        }
      """, TestWorlds.empty, RecordType(List(AttrType("_1", IntType()), AttrType("_2", BoolType())), None))
  }

  test("""let-polymorphism #2""") {
    success(
      """
        {
          f := \x -> (x, 12);
          (f(1), f(true))
        }
      """, TestWorlds.empty,
      RecordType(List(
        AttrType("_1", RecordType(List(AttrType("_1",  IntType()), AttrType("_2", IntType())), None)),
        AttrType("_2", RecordType(List(AttrType("_1",  BoolType()), AttrType("_2", IntType())), None))),
        None))
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
    val rec = ConstraintRecordType(Set(AttrType("age", age)))
    val rec2 = ConstraintRecordType(Set(AttrType("age", BoolType())))
    success(
      """
        {
        f := \x -> x = x;
        g := \x -> x.age;
        (f, g, if true then f else g)
        }
      """, TestWorlds.empty, RecordType(Seq(
      AttrType("_1", FunType(x, BoolType())),
      AttrType("_2", FunType(rec, age)),
      AttrType("_3", FunType(rec2, BoolType()))), None))
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
      FunType(IntType(), SetType(IntType())))
  }

  test("""let-polymorphism #7""") {
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
      """, TestWorlds.empty, AnyType())
  }

  test("map") {
    success(
      """
        {
        map := \(col, f) -> for (el <- col) yield list f(el);
        col1 := set(1,2,3);
        col2 := list(1.0, 2.0, 3.0);
        (map(col1, \x -> x + 1), map(col2, \x -> x + 1.1))
        }
      """, TestWorlds.empty, RecordType(List(AttrType("_1", ListType(IntType())), AttrType("_2", ListType(FloatType()))), None))
  }

  test("""\(x, y) -> x.age = y""") {
    val y = TypeVariable()
    success("""\(x, y) -> x.age = y""", TestWorlds.empty, FunType(RecordType(List(AttrType("_1", ConstraintRecordType(Set(AttrType("age", y)))), AttrType("_2", y)), None), BoolType()))
  }

  test("""\(x, y) -> (x, y)""") {
    val x = TypeVariable()
    val y = TypeVariable()
    val rec = RecordType(List(AttrType("_1", x), AttrType("_2", y)), None)
    success("""\(x, y) -> (x, y)""", TestWorlds.empty, FunType(rec, rec))
  }

  test("""\(x,y) -> for (z <- x) yield sum y(z)""") {
    val z = TypeVariable()
    val yz = NumberType()
    success("""\(x,y) -> for (z <- x) yield sum y(z)""", TestWorlds.empty,
      FunType(
        RecordType(List(AttrType("_1",ConstraintCollectionType(z, None, None)), AttrType("_2", FunType(z, yz))), None),
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

}
