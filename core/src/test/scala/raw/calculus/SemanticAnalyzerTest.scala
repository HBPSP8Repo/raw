package raw
package calculus

class SemanticAnalyzerTest extends CoreTest {

  import raw.calculus.Calculus._

  def go(query: String, world: World) = {
    val ast = parse(query)
    val t = new Calculus.Calculus(ast)
    logger.debug(s"AST: ${t.root}")
    logger.debug(s"Parsed tree: ${CalculusPrettyPrinter(t.root)}")

    val analyzer = new SemanticAnalyzer(t, world, query)
    analyzer.errors.foreach{ case err => logger.debug(s"Error: ${ErrorsPrettyPrinter(err)}")}
    analyzer
  }

  case class Mappings(mMap: Map[Monoid, Monoid]=Map(), tMap: Map[Type, Type]=Map()) {
    def ++(m2: Mappings): Mappings = {
      val nmmap = this.mMap ++ m2.mMap
      // we should not see different variables pointing to the same one
      // TODO report which one is duplicated, something like that
      assert(nmmap.keys.toSet.size == nmmap.values.toSet.size)
      val ntmap = this.tMap ++ m2.tMap
      // we should not see different variables pointing to the same one
      // TODO report which one is duplicated, something like that
      assert(ntmap.keys.toSet.size == ntmap.values.toSet.size)
      Mappings(nmmap, ntmap)
    }
  }

  abstract class Check()
  case class MProp(m: Monoid, c: Option[Boolean], i: Option[Boolean]) extends Check

  def success(query: String, world: World, expectedType: Type, checks: Set[Check]=Set()) = {
    val analyzer = go(query, world)
    analyzer.logMonoidsGraph()
    analyzer.logConcatProperties()
    assert(analyzer.errors.isEmpty)
    val inferredType = analyzer.tipe(analyzer.tree.root)
    analyzer.errors.foreach{ case err => logger.debug(s"Error: ${ErrorsPrettyPrinter(err)}")}
    analyzer.printTypedTree()
    logger.debug(s"Actual type: ${FriendlierPrettyPrinter(inferredType)}")
    logger.debug(s"Expected type: ${FriendlierPrettyPrinter(expectedType)}")

    def checkStructure(t1: Type, t2: Type, map: Mappings=Mappings()): Mappings = (t1, t2) match {
      case (t1, t2) if t1 == t2 => Mappings()
      case (p1: VariableType, p2: VariableType) => {
        if (map.tMap.contains(p1)) {
          assert(map.tMap(p1) == p2)
          Mappings()
        } else {
          Mappings(Map(), Map(p1 -> p2))
        }
      }
      case (r1: RecordType, r2: RecordType) =>
        (r1.recAtts, r2.recAtts) match {
          case (as1: Attributes, as2: Attributes) =>
            assert(as1.atts.length == as2.atts.length)
            as1.atts.zip(as2.atts).map{
              case (a1,a2) =>
                assert(a1.idn == a2.idn)
                checkStructure(a1.tipe, a2.tipe, map)
            }.foldLeft(Mappings()){case (m1: Mappings, m2: Mappings) => m1 ++ m2}
          case (as1: AttributesVariable, as2: AttributesVariable) =>
            assert(as1.atts.size == as2.atts.size)
            as1.atts.map{
              a1 =>
                assert(as2.getType(a1.idn).isDefined)
                checkStructure(a1.tipe, as2.getType(a1.idn).get, map)
            }.foldLeft(Mappings()){case (m1: Mappings, m2: Mappings) => m1 ++ m2}
          case (as1: ConcatAttributes, as2: ConcatAttributes) =>
            if (map.tMap.contains(r1)) {
              assert(map.tMap(r1) == r2)
              Mappings()
            } else {
              Mappings(Map(), Map(r1 -> r2))
            }


        }
      case (c1: CollectionType, c2: CollectionType) =>
        ((c1.m, c2.m) match {
          case (v1: MonoidVariable, v2: MonoidVariable) =>
            if (map.mMap.contains(c1.m)) {
              assert(map.mMap(c1.m) == c2.m)
              Mappings()
            } else {
              Mappings(Map(c1.m -> c2.m), Map())
            }
          case (x1, x2) if x1 == x2 => Mappings()
        }) ++ checkStructure(c1.innerType, c2.innerType, map)
      case (f1: FunType, f2: FunType) =>
        checkStructure(f1.t1, f2.t1) ++ checkStructure(f1.t2, f2.t2)
      case (p1: PatternType, p2: PatternType) =>
        assert(p1.atts.length == p2.atts.length)
        p1.atts.zip(p2.atts).map{
          case (a1, a2) => checkStructure(a1.tipe, a2.tipe)
        }.foldLeft(Mappings()){case (m1: Mappings, m2: Mappings) => m1 ++ m2}
      case (t, u: UserType) => checkStructure(t, analyzer.world.tipes(u.sym))
      case (u: UserType, t) => checkStructure(analyzer.world.tipes(u.sym), t)
    }




    val mappings = checkStructure(expectedType, inferredType)
    logger.debug(s"mappings: $mappings")
    for (check <- checks) check match {
      case MProp(m, c, i) =>
        val rProps = analyzer.monoidProps(mappings.mMap(m))
        assert(c == rProps.commutative)
        assert(i == rProps.idempotent)
    }
//    assert(compare(inferredType.toString, expectedType.toString))
//    assert(typesEq(inferredType, expectedType))
  }

  private def typesEq(t1: Type, t2: Type): Boolean =
    compare(PrettyPrinter(t1), PrettyPrinter(t2))
  
  def failure(query: String, world: World, error: CalculusError) = {
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
          case x@UnexpectedType(t1, expected1, _, _) if typesEq(t, t1) && typesEq(expected, expected1) =>

            logger.debug(s"begin at ${x.pos.get.begin}; end at ${x.pos.get.end}; string is $query")

            true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")

      case IncompatibleMonoids(m, t, _) =>
        assert(analyzer.errors.exists {
          case IncompatibleMonoids(`m`, `t`, _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case UnknownDecl(IdnUse(x), _) =>
        assert(analyzer.errors.exists {
          case UnknownDecl(IdnUse(`x`), _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case MultipleDecl(IdnDef(x), _) =>
        assert(analyzer.errors.exists {
          case MultipleDecl(IdnDef(`x`), _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case _: UnknownPartition =>
        assert(analyzer.errors.exists {
          case _: UnknownPartition => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case AmbiguousIdn(IdnUse(x), _) =>
        assert(analyzer.errors.exists {
          case AmbiguousIdn(IdnUse(`x`), _) => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case _: IllegalStar =>
        assert(analyzer.errors.exists {
          case _: IllegalStar => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case _: InvalidRegexSyntax =>
        assert(analyzer.errors.exists {
          case _: InvalidRegexSyntax => true
          case _ => false
        }, s"Error '${ErrorsPrettyPrinter(error)}' not contained in errors")
      case _: InvalidDateTimeFormatSyntax =>
        assert(analyzer.errors.exists {
          case _: InvalidDateTimeFormatSyntax => true
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
    success("for (i <- Items) yield set i", TestWorlds.linked_list, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
  }

  test("for (i <- Items) yield set i.next") {
    success("for (i <- Items) yield set i.next", TestWorlds.linked_list, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
  }

  test("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x") {
    success("for (i <- Items; x := i; x.value = 10; x.next != x) yield set x", TestWorlds.linked_list, CollectionType(SetMonoid(),UserType(Symbol("Item"))))
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
    val m = MonoidVariable()
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
      FunType(PatternType(List(PatternAttrType(CollectionType(m, z)), PatternAttrType(FunType(z, n)))), n),
      Set(MProp(m, None, Some(false))))
  }

  test("""home-made count applied to wrong type""") {
    val mv = MonoidVariable()
    failure(
      """
        {
        count1 := \x -> for (z <- x) yield sum 1
        count1(1)
        }

      """, TestWorlds.empty,
      IncompatibleTypes(CollectionType(mv, TypeVariable()), IntType()))
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
      RecordType(Attributes(List(AttrType("f", FunType(n1, FunType(n1, BoolType()))), AttrType("_2", FunType(IntType(), BoolType()))))))
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
    val m = MonoidVariable()
    val z = TypeVariable()
    val yz = NumberType()
    success("""\(x,y) -> for (z <- x) yield sum y(z)""", TestWorlds.empty,
      FunType(
        PatternType(List(PatternAttrType(CollectionType(m, z)), PatternAttrType(FunType(z, yz)))),
        yz),
      Set(MProp(m, None, Some(false))))
  }

  test("""\(x,y) -> for (z <- x) yield max y(z)""") {
    val m = MonoidVariable()
    val z = TypeVariable()
    val yz = NumberType()
    success("""\(x,y) -> for (z <- x) yield max y(z)""", TestWorlds.empty,
      FunType(
        PatternType(List(PatternAttrType(CollectionType(m, z)), PatternAttrType(FunType(z, yz)))),
        yz),
      Set(MProp(m, None, None)))
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
          PatternAttrType(PatternType(List(
            PatternAttrType(IntType()), PatternAttrType(IntType())))))), ob))
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
    val m = MonoidVariable()
    success("select s from s in students", TestWorlds.professors_students, CollectionType(m, UserType(Symbol("student"))),
      Set(MProp(m, None, None)))
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
    val m = MonoidVariable()
    success("""select s.lastname from s in (select s.name as lastname from s in students)""",
      TestWorlds.professors_students, CollectionType(m, StringType()), Set(MProp(m, None, None)))
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
    success("select s.age, partition from students s group by s.age", TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(
                                                  AttrType("age", IntType()),
                                                  AttrType("partition", CollectionType(MonoidVariable(), UserType(Symbol("student")))))))))
  }

  test("select partition from students s group by s.age") {
    success("select partition from students s group by s.age", TestWorlds.professors_students,
      CollectionType(MonoidVariable(), CollectionType(MonoidVariable(), UserType(Symbol("student")))))
  }

  test("select s.age, (select p.name from partition p) from students s group by s.age") {
    success("select s.age, (select p.name from partition p) as names from students s group by s.age", TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("names", CollectionType(MonoidVariable(), StringType())))))))
  }

  test("select s.dept, count(partition) as n from students s group by s.dept") {
    success("select s.department, count(partition) as n from students s group by s.department", TestWorlds.school,
      CollectionType(MonoidVariable(),RecordType(Attributes(List(AttrType("department", StringType()), AttrType("n", IntType()))))))
  }

  ignore("select dpt, count(partition) as n from students s group by dpt: s.dept") {
    success("select dpt, count(partition) as n from students s group by dpt: s.dept", TestWorlds.professors_students, ???)
  }

  test("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10") {
    success("select s.age/10 as decade, (select s.name from partition s) as names from students s group by s.age/10", TestWorlds.professors_students,
      CollectionType(MonoidVariable(),RecordType(Attributes(List(AttrType("decade",IntType()), AttrType("names",CollectionType(MonoidVariable(),StringType())))))))
  }

  test("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age") {
    success("select s.age, (select s.name, partition from partition s group by s.name) as names from students s group by s.age",
      TestWorlds.professors_students, CollectionType(MonoidVariable(),RecordType(Attributes(List(AttrType("age",IntType()),
            AttrType("names",CollectionType(MonoidVariable(),RecordType(Attributes(List(AttrType("name",StringType()), AttrType("partition",
              CollectionType(MonoidVariable(), UserType(Symbol("student"))))))))))))))
  }

  test("sum(list(1))") {
    success("sum(list(1))", TestWorlds.empty, IntType())
  }

  test("sum(list(1.1))") {
    success("sum(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("sum(1)") {
    // TODO: failure should also check monoid properties
    failure("sum(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(MonoidVariable(), NumberType())))
  }

  test("max(list(1))") {
    success("max(list(1))", TestWorlds.empty, IntType())
  }

  test("max(list(1.1))") {
    success("max(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("max(1)") {
    failure("max(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(MonoidVariable(), NumberType())))
  }

  test("min(list(1))") {
    success("min(list(1))", TestWorlds.empty, IntType())
  }

  test("min(list(1.1))") {
    success("min(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("min(1)") {
    failure("min(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(MonoidVariable(), NumberType())))
  }

  test("avg(list(1))") {
    success("avg(list(1))", TestWorlds.empty, IntType())
  }

  test("avg(list(1.1))") {
    success("avg(list(1.1))", TestWorlds.empty, FloatType())
  }

  test("avg(1)") {
    failure("avg(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(MonoidVariable(), NumberType())))
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
    failure("count(1)", TestWorlds.empty, UnexpectedType(IntType(), CollectionType(MonoidVariable(), TypeVariable())))
  }

  test("to_bag(list(1))") {
    success("""to_bag(list(1))""", TestWorlds.empty, CollectionType(BagMonoid(), IntType()))
  }

  ignore("to_set(list(1))") {
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
      """.stripMargin, TestWorlds.professors_students, RecordType(Attributes(List(AttrType("_1", CollectionType(MonoidVariable(), IntType())),
        AttrType("_2", CollectionType(MonoidVariable(), IntType()))))))
  }

  test("polymorphic partition") {
    success(
      """
        |{
        |  a := \xs -> select x.age, count(partition) from x in xs where x.age > 15 group by x.age;
        |  (a(students), a(professors))
        |}
      """.stripMargin, TestWorlds.professors_students,
      RecordType(Attributes(List(AttrType("_1", CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", IntType())))))),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", IntType()))))))
      ))))
  }

  test("polymorphic partition #2") {
    success(
      """
        |{
        |  a := \xs -> select x.age, partition from x in xs where x.age > 15 group by x.age;
        |  (a(students), a(professors))
        |}
      """.stripMargin, TestWorlds.professors_students,
        RecordType(Attributes(List(
          AttrType("_1", CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("partition", CollectionType(MonoidVariable(), UserType(Symbol("student"))))))))),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("partition", CollectionType(MonoidVariable(), UserType(Symbol("professor")))))))))
      ))))
  }

  test("polymorphic partition #3") {
    val xs = RecordType(AttributesVariable(Set(AttrType("a", IntType()))))
    success(
      """
        |{
        |  a := \xs -> select x.a, partition from x in xs where x.a > 15 group by x.a;
        |  a
        |}
      """.stripMargin, TestWorlds.things,
      FunType(CollectionType(MonoidVariable(), xs),
              CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("a", IntType()), AttrType("partition", CollectionType(MonoidVariable(), xs))))))))
  }

  test("blablabla") {
    val input: Iterable[(Int, String)] = for (x <- Iterable(1,2,3,4); y <- Iterable("1","2","3","4")) yield (x,y)
    val g = input.groupBy{case i => i._1}.map{case (k, vs) => (k, vs.map(_._2))}
    logger.debug(g.toString)
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

  ignore("#87 (a: 1, a: 2)") {
    failure("(a: 1, a: 2)", TestWorlds.empty, AmbiguousIdn(IdnUse("a")))
  }

  ignore("#87 { a := 1; (a, a) }") {
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
      CollectionType(MonoidVariable(),
        RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(MonoidVariable(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))

  }

  test("""\xs -> select x from x in xs (monoid doesn't remain)""") {
    val mxs = MonoidVariable()
    val mout = MonoidVariable()
    val t = TypeVariable()
    success("""\xs -> select x from x in xs""", TestWorlds.empty, FunType(CollectionType(mxs, t), CollectionType(mout, t)))
  }

  test("select x from x in xs (applied to students)") {
    val m = MonoidVariable()
    success("""{
       a := \xs -> select x from x in xs;
       a(students)
       } """, TestWorlds.professors_students,
      CollectionType(m, UserType(Symbol("student"))),
      Set(MProp(m, None, None)))

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
        CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(MonoidVariable(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()))))))))))),
                                 AttrType("_2",
        CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()),
          AttrType("partition", CollectionType(MonoidVariable(),
            RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType())))))))))))))))
  }

  test("select * from students") {
    success(
      "select * from students",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), UserType(Symbol("student"))))
  }

  test("select * from students, professors") {
    success(
      "select * from students, professors",
      TestWorlds.professors_students,
    // TODO: Fix!!!
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()),
                                                                  AttrType("name_1", StringType()), AttrType("age_1", IntType())))))
    )
  }

  test("select * from students, p in professors") {
    success(
      "select * from students, p in professors",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("age", IntType()),
        AttrType("name_1", StringType()), AttrType("age_1", IntType())))))
    )
  }

  test("select a.name from a in (select * from students, p in professors)") {
    success(
      "select a.name from a in (select * from students, p in professors)",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), StringType())
    )
  }


  test("{ a:= select * from students, p in professors; select name from a }") {
    success(
      "{ a:= select * from students, p in professors; select name from a }",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), StringType()))
  }


  test("select * from i in list(1,2,3)") {
    success(
      "select * from i in list(1,2,3)",
      TestWorlds.empty,
      CollectionType(MonoidVariable(), IntType())
    )
  }

  test("select * from i in list(1,2,3), j in list(1,2,3)") {
    success(
      "select * from i in list(1,2,3), j in list(1,2,3)",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("i", IntType()), AttrType("j", IntType())))))
    )
  }

  test("select * from list(1,2,3), j in list(1,2,3)") {
    success(
      "select * from list(1,2,3), j in list(1,2,3)",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("j", IntType())))))
    )
  }

  test("select * from list(1,2,3), list(1,2,3)") {
    success(
      "select * from list(1,2,3), list(1,2,3)",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("_1", IntType()), AttrType("_2", IntType())))))
    )
  }

  test("select * from i in list(1,2,3), students") {
    success(
      "select * from i in list(1,2,3), students",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("i", IntType()), AttrType("name", StringType()), AttrType("age", IntType())))))
    )
  }

  test("select age, * from students") {
    failure(
      "select age, * from students",
      TestWorlds.professors_students,
      IllegalStar(null))
  }

  test("select age, * from students group by age") {
    success(
      "select age, * from students group by age",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", CollectionType(MonoidVariable(), UserType(Symbol("student")))))))))
  }

  test("select age, count(*) from students") {
    failure(
      "select age, count(*) from students",
      TestWorlds.professors_students,
      IllegalStar(null))
  }

  test("select age, count(*) from students group by age") {
    success(
      "select age, count(*) from students group by age",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", IntType()))))))
  }

  test("select age, (select name from partition), count(*), max(select age from partition) from students group by age") {
    success(
      "select age, (select name from partition), count(*), max(select age from partition) from students group by age",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(),
        RecordType(Attributes(List(AttrType("age", IntType()), AttrType("_2", CollectionType(MonoidVariable(), StringType())), AttrType("_3", IntType()), AttrType("_4", IntType()))))))
  }

  test("select name, count(*) from students group by age") {
    success(
      "select name, count(*) from students group by age",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("name", StringType()), AttrType("_2", IntType()))))))
  }

  ignore("#58 for ( <- students) yield list *") {
    success(
      "for ( <- students) yield list *",
      TestWorlds.professors_students,
      UserType(Symbol("students")))
  }

  test("list(1,2,3)") {
    success("list(1, 2, 3)", TestWorlds.empty, CollectionType(ListMonoid(), IntType()))
  }

  test("""set(("dbname", "authors"), ("dbname", "publications"))""") {
    success(
      """set(("dbname", "authors"), ("dbname", "publications"))""",
      TestWorlds.empty,
      CollectionType(SetMonoid(), RecordType(Attributes(List(AttrType("_1", StringType()), AttrType("_2", StringType()))))))
  }

  test("list over comprehension") {
    val t = TypeVariable()
    success(
      """\xs -> for (x <- xs) yield list x""",
      TestWorlds.empty,
      FunType(
        CollectionType(MonoidVariable(), t),
        CollectionType(ListMonoid(), t)))
  }

  test("list over polymorphic comprehension") {
    val t = TypeVariable()
    val m = MonoidVariable()
    success(
      """
        |{
        | a := \xs -> for (x <- xs) yield list x;
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        CollectionType(m, t),
        CollectionType(ListMonoid(), t)),
      Set(MProp(m, Some(false), Some(false))))
  }

  test("2x list over polymorphic comprehension") {
    val t1 = TypeVariable()
    val t2 = TypeVariable()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
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
          CollectionType(m1, t1),
          CollectionType(ListMonoid(), t1))),
        AttrType("field2", FunType(
          CollectionType(m2, t2),
          CollectionType(ListMonoid(), t2)))))), Set(MProp(m1, Some(false), Some(false)), MProp(m2, Some(false), Some(false))))
  }

  test("sum over polymorphic comprehension #1") {
    val n = NumberType()
    val m = MonoidVariable()
    success(
      """
        |{
        | a := \xs -> for (x <- xs) yield sum x;
        | a
        |}
      """.stripMargin,
    TestWorlds.empty,
    FunType(CollectionType(m, n), n), Set(MProp(m, None, Some(false))))
  }

  test("sum over polymorphic comprehension #2") {
    val n = NumberType()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
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
          PatternAttrType(CollectionType(m1, n)),
          PatternAttrType(CollectionType(m2, TypeVariable())))),
        n), Set(MProp(m1, None, Some(false)), MProp(m2, None, Some(false))))
  }

  test("bag over polymorphic comprehension") {
    val t = TypeVariable()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
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
          PatternAttrType(CollectionType(m1, t)),
          PatternAttrType(CollectionType(m2, TypeVariable())))),
        CollectionType(BagMonoid(), t)), Set(MProp(m1, None, Some(false)), MProp(m2, None, Some(false))))
  }


  test("sum over polymorphic select") {
    val n = NumberType()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
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
          PatternAttrType(CollectionType(m1, n)),
          PatternAttrType(CollectionType(m2, TypeVariable())))),
        n), Set(MProp(m1, None, Some(false)), MProp(m2, None, Some(false))))
  }

  test("sum over polymorphic select #1") {
    val n = NumberType()
    val m = MonoidVariable()
    success(
      """
        |{
        | a := \xs -> sum(xs);
        | a
        |}
      """.stripMargin,
      TestWorlds.empty,
      FunType(
        CollectionType(m, n),
        n), Set(MProp(m, None, Some(false))))
  }

  test("select x from x in students, y in professors") {
    success(
      """select x from x in students, y in professors""",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), UserType(Symbol("student"))))
  }

  test("""xs -> sum(select x.age from x in students, y in xs)""") {
    val m = MonoidVariable()
    success(
      """\xs -> sum(select x.age from x in students, y in xs)""",
      TestWorlds.professors_students,
      FunType(CollectionType(m, TypeVariable()), IntType()), Set(MProp(m, None, Some(false))))
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

  test("""\xs -> sum(xs)""") {
    val mv = MonoidVariable()
    val n = NumberType()
    success(
      """\xs -> sum(xs)""",
      TestWorlds.empty,
      FunType(CollectionType(mv, n), n))
  }

  test("""\xs -> sum(xs), xs union xs""") {
    failure(
      """\xs -> sum(xs), xs union xs""",
      TestWorlds.empty,
      UnexpectedType(CollectionType(MonoidVariable(), NumberType()), CollectionType(SetMonoid(), TypeVariable())))
  }

  test("""\xs,ys -> sum(xs), ys union ys""") {
    val m = MonoidVariable()
    val n = NumberType()
    val v = TypeVariable()
    val xs = CollectionType(m, n)
    val ys = CollectionType(SetMonoid(), v)
    success(
      """\xs,ys -> sum(xs), ys union ys""",
      TestWorlds.empty,
      FunType(PatternType(List(PatternAttrType(xs), PatternAttrType(ys))), RecordType(Attributes(List(AttrType("_1", n), AttrType("_2", ys))))),
      Set(MProp(m, None, Some(false))))
  }

  test("select A from authors A") {
    success(
      "select A from authors A",
      TestWorlds.publications,
      CollectionType(MonoidVariable(), UserType(Symbol("Author"))))
  }

  ignore("#106 inference through bind") {
    success(
      """
        |
        |   \ys -> {
        |     A := (select a from a in ys); // later we see ys is union with a set, so we should be min'd by a set
        |     x := ys union set(1,2,3);
        |     (A, x)
        |     }
        |
      """.stripMargin,
      TestWorlds.empty,
      FunType(CollectionType(SetMonoid(), IntType()),
          RecordType(Attributes(List(AttrType("A", CollectionType(MonoidVariable(), IntType())), AttrType("x", CollectionType(SetMonoid(), IntType()))))))
    )
  }


  test("......#3") {
    val y = TypeVariable()
    success(
      """
        |
        |   \ys -> {
        |     A := (select a from a in ys);
        |     A}
        |
      """.stripMargin,
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), y), CollectionType(MonoidVariable(), y))
    )
  }

  ignore("#106 inference through bind #2") {
    success(
    """
      |
      |   \ys -> {
      |     A := (select a from a in ys); // later we see ys is union with a set, so we should be min'd by a set
      |     B := sum(A);                  // but we do sum of A
      |     (ys union set(1,2,3), A, B)
      |     }
      |
    """.stripMargin,
    TestWorlds.empty,
    FunType(CollectionType(SetMonoid(), IntType()), RecordType(Attributes(List(
      AttrType("_1", CollectionType(SetMonoid(), IntType())),
      AttrType("A", CollectionType(MonoidVariable(), IntType())),
      AttrType("B", IntType())
    ))))
    )
  }

  test("free variables #1. Check that only the xs inner and monoid are free") {
    val n1 = NumberType()
    val n2 = NumberType()
    success(
      """
        |{
        |  a := \xs -> sum(select x from x in xs);
        |  (a1: a, a2: a, a(list(1)))
        |}
      """.stripMargin, TestWorlds.empty,
      RecordType(Attributes(List(
        AttrType("a1", FunType(CollectionType(MonoidVariable(), n1), n1)),
        AttrType("a2", FunType(CollectionType(MonoidVariable(), n2), n2)),
        AttrType("_3", IntType())
      ))))
  }

  test("free variables #2. What should be the output type of a?") {
    val n1 = NumberType()
    val n2 = NumberType()
    val n3 = NumberType()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
    val m3 = MonoidVariable()
    success(
      """
        |{
        |  a := \xs -> { b := \ys -> sum(select x from x in ys); b };
        |  (a(true),a(1.2),a("tralala"))
        |}
      """.stripMargin, TestWorlds.empty,
        RecordType(Attributes(List(
          AttrType("_1", FunType(CollectionType(m1, n1), n1)),
          AttrType("_2", FunType(CollectionType(m2, n2), n2)),
          AttrType("_3", FunType(CollectionType(m3, n3), n3))
        ))), Set(MProp(m1, None, Some(false)), MProp(m2, None, Some(false)), MProp(m3, None, Some(false))))
  }

  test("free variables #3. What should be the output type of a?") {
    val n1 = NumberType()
    val n2 = NumberType()
    val n3 = NumberType()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
    val m3 = MonoidVariable()
    success(
      """
        |{
        |  a := \xs -> (\ys -> sum(select x from x in ys));
        |  (a1: a, a2: a, a3: a)
        |}
      """.stripMargin, TestWorlds.empty,
        RecordType(Attributes(List(
          AttrType("a1", FunType(TypeVariable(), FunType(CollectionType(m1, n1), n1))),
          AttrType("a2", FunType(TypeVariable(), FunType(CollectionType(m2, n2), n2))),
          AttrType("a3", FunType(TypeVariable(), FunType(CollectionType(m3, n3), n3)))
        ))))
  }

  test("free variables #4") {
    val n1 = NumberType()
    val n2 = NumberType()
    val n3 = NumberType()
    success(
      """
        |{
        |  a := list((\x -> x+x), (\x -> x+x+x));
        |  (a1: a, a2: a, a3: a)
        |}
      """.stripMargin, TestWorlds.empty,
      RecordType(Attributes(List(
        AttrType("a1", CollectionType(ListMonoid(), FunType(n1, n1))),
        AttrType("a2", CollectionType(ListMonoid(), FunType(n2, n2))),
        AttrType("a3", CollectionType(ListMonoid(), FunType(n3, n3)))
      ))))
  }


  ignore("#107 free variables #5 (not too sure of the type)") {
    val n1 = NumberType()
    val m1 = MonoidVariable()
    val m2 = MonoidVariable()
    success(
      """
        |{
        |  a := \xs -> {
        |    b := \xs2 -> sum(select x + y from x in xs, y in xs2);
        |    b
        |  };
        |  (a, a(list(1.2, 1.3)), a(list(1,2,3)))
        |}
      """.stripMargin, TestWorlds.empty,
      RecordType(Attributes(List(
        AttrType("a", FunType(CollectionType(m1, n1), FunType(CollectionType(m2, n1), n1))),
        AttrType("_2", FunType(CollectionType(MonoidVariable(), FloatType()), FloatType())),
        AttrType("_3", FunType(CollectionType(MonoidVariable(), IntType()), IntType())))
      )))

  }

  test("""\xs -> { a := max(xs); v1: a, v2: a }""") {
    val n = NumberType()
    success(
      """\xs -> { a := max(xs); v1: a, v2: a }""",
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), n), RecordType(Attributes(List(AttrType("v1", n), AttrType("v2", n))))))
  }


  test("""{ b := \xs -> { a := max(xs); v1: a, v2: a }; b }""") {
    val n = NumberType()
    success(
      """{ b := \xs -> { a := max(xs); v1: a, v2: a }; b }""",
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), n), RecordType(Attributes(List(AttrType("v1", n), AttrType("v2", n))))))
  }

  test("""\xs, ys -> select x.age, * from x in xs, ys group by x.age""") {
    val age = TypeVariable()
    val xs = CollectionType(MonoidVariable(), RecordType(AttributesVariable(Set(AttrType("age", age)))))
    val ys = CollectionType(MonoidVariable(), TypeVariable())
    val star = CollectionType(MonoidVariable(), RecordType(ConcatAttributes()))
    success(
      """\xs, ys -> select x.age, * from x in xs, ys group by x.age""",
      TestWorlds.empty,
      FunType(PatternType(List(PatternAttrType(xs), PatternAttrType(ys))),
              CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("age", age), AttrType("_2", star)))))))
  }


  test("""(\xs, ys -> select x.age, * from x in xs, ys group by x.age)(students, professors)""") {
    success(
      """(\xs, ys -> select x.age, * from x in xs, ys group by x.age)(students, professors)""",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(
        AttrType("age", IntType()),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("name", StringType()),
          AttrType("age", IntType()),
          AttrType("name_1", StringType()),
          AttrType("age_1", IntType())
          ))))))))))
  }

  test("""(\xs, ys -> select x.age, count(*) as c, * from x in xs, ys group by x.age)(students, professors)""") {
    success(
      """(\xs, ys -> select x.age, count(*) as c, * from x in xs, ys group by x.age)(students, professors)""",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(
        AttrType("age", IntType()),
        AttrType("c", IntType()),
        AttrType("_3", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("name", StringType()),
          AttrType("age", IntType()),
          AttrType("name_1", StringType()),
          AttrType("age_1", IntType())
        ))))))))))
  }

  test("""select x.name_1 from x in ((\xs, ys -> select * from x in xs, ys)(students, professors))""") {
    success(
      """select x.name_1 from x in ((\xs, ys -> select * from x in xs, ys)(students, professors))""",
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), StringType()))
  }

  test("""applying func to a field""") {
    // would fail because of wrong resolving of constraint at fun app time
    // this is a bug Ben found while Miguel was in Bern
    success(
      """{
        f := \x -> x.age > 73;
        select f(x) from x in students
      }
      """,
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), BoolType()))
  }

  test("""#108 resolve a record type from a function pattern""") {
    // f2 below takes three parameters.
    success(
      """{
        f := \xyz -> { f2 := \(n,t,y) -> y > 10; select f2(x) from x in xyz }
        f
      }
      """,
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), PatternType(List(PatternAttrType(TypeVariable()), PatternAttrType(TypeVariable()), PatternAttrType(BoolType())))), CollectionType(MonoidVariable(), BoolType()))
      )
  }

  test("""#108.2 resolve a record type from a function pattern""") {
    // f2 below takes three parameters.
    success(
      """{
        f := \xyz -> { f2 := \(n,t,y) -> y > 10; x := (1,2,3) ; f2(x) }
        f
      }
      """,
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), PatternType(List(PatternAttrType(TypeVariable()), PatternAttrType(TypeVariable()), PatternAttrType(BoolType())))), CollectionType(MonoidVariable(), BoolType()))
    )
  }
  test("""#108.3 resolve a record type from a function pattern""") {
    // f2 below takes three parameters.
    success(
      """{
        f := \f2-> { x := (a:1,b:2,c:3) ; f2(x) }
        myf := \x -> x.a + x.b + x.c
        f(myf)
        f <> (1,2,3) -> f with 3 args
        f((1,2,3)) -> f with single record
        check record cons!!!
      }
      """,
      TestWorlds.empty,
      FunType(CollectionType(MonoidVariable(), PatternType(List(PatternAttrType(TypeVariable()), PatternAttrType(TypeVariable()), PatternAttrType(BoolType())))), CollectionType(MonoidVariable(), BoolType()))
    )
  }
  /*

group_by_age(xs) := select x.age, * from x in xs group by x.age



  List(IncompatibleTypes(
    CollectionType(MonoidVariable(Symbol($8)),RecordType(Attributes(List(
      AttrType(age,IntType()),
      AttrType(_2,CollectionType(MonoidVariable(Symbol($24)),RecordType(ConcatAttributes(Symbol($23))))))))),
    CollectionType(MonoidVariable(Symbol($8)),RecordType(Attributes(List(
      AttrType(age,IntType()),
      AttrType(_2,CollectionType(MonoidVariable(Symbol($24)),RecordType(ConcatAttributes(Symbol($23))))))))),


   */

  test("polymorphic select w/ concat attributes") {
    val ageType = TypeVariable()
    val starType = RecordType(Attributes(Vector(AttrType("name",StringType()), AttrType("age",IntType()), AttrType("name_1",StringType()), AttrType("age_1",IntType()))))
    success(
      """
        |{
        |  group_by_age := \xs, ys -> select x.age, * from x in xs, ys group by x.age;
        |  (group_by_age, group_by_age(students, professors), group_by_age(professors, students))
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      RecordType(Attributes(List(
        AttrType("group_by_age",
          FunType(
            PatternType(List(PatternAttrType(CollectionType(MonoidVariable(),RecordType(AttributesVariable(Set(AttrType("age",ageType)))))),
                             PatternAttrType(CollectionType(MonoidVariable(),TypeVariable())))),
            CollectionType(MonoidVariable(),
              RecordType(Attributes(List(
                AttrType("age",ageType),
                AttrType("_2", CollectionType(MonoidVariable(),RecordType(ConcatAttributes()))))))))),
        AttrType("_2",
          CollectionType(MonoidVariable(),
            RecordType(Attributes(List(
              AttrType("age", IntType()),
              AttrType("_2", CollectionType(MonoidVariable(), starType))))))),
        AttrType("_3",
          CollectionType(MonoidVariable(),
            RecordType(Attributes(List(
              AttrType("age", IntType()),
              AttrType("_2", CollectionType(MonoidVariable(), starType)))))))))))
  }

  ignore("#109 polymorphic select w/ concat attributes #2") {
    success(
      """
        |{
        |  group_by_age := \xs, ys -> select x.age, * from x in xs, ys group by x.age;
        |  (group_by_age(students, list(1,2,3)), group_by_age(students, professors))
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      RecordType(Attributes(List(
        AttrType("_1", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("_3", IntType()))
          ))))))))),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("name_1", StringType()),
            AttrType("age_1", IntType())
          )))))
        )))
      ))))))
  }

  test("polymorphic select w/ concat attributes #3") {
    success(
      """
        |{
        |  group_by_age := \xs, ys -> select x.age, * from x in xs, foo in ys group by x.age;
        |  (group_by_age(students, list(1,2,3)), group_by_age(students, professors))
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      RecordType(Attributes(List(
        AttrType("_1", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("foo", IntType()))
          ))))))))),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("name_1", StringType()),
            AttrType("age_1", IntType())
          )))))
        )))
        ))))))
  }

  test("polymorphic select w/ concat attributes #4") {
    success(
      """
        |{
        |  group_by_age := \xs, ys -> select x.age, * from x in xs, age in ys group by x.age;
        |  (group_by_age(students, list(1,2,3)), group_by_age(students, professors))
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      RecordType(Attributes(List(
        AttrType("_1", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("age_1", IntType()))
          ))))))))),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("age", IntType()),
          AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("name_1", StringType()),
            AttrType("age_1", IntType())
          )))))
        )))
        ))))))
  }


  ignore("#110 polymorphic select w/ concat attributes #5") {
    success(
      """
        |{
        |  group_by_age := \xs, ys -> select x.age, * from x in xs, age in ys group by x.age;
        |  group_by_age(students, professors) append group_by_age(professors, students)
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      CollectionType(ListMonoid(), RecordType(Attributes(List(
        AttrType("age", IntType()),
        AttrType("_2", CollectionType(MonoidVariable(),
        RecordType(Attributes(List(
            AttrType("name", StringType()),
            AttrType("age", IntType()),
            AttrType("name_1", StringType()),
            AttrType("age_1", IntType())
          ))))))))))
  }

  test("........") {
    success(
      """
        |{
        | group_by_first := \xs, ys -> select x, * from x in xs, y in ys where x = y.age group by x;
        | group_by_first(list(1,2,3),students)
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(
        AttrType("x", IntType()),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("x", IntType()),
          AttrType("name", StringType()),
          AttrType("age", IntType())
        )))))
      )))))
  }


  test("..........") {
    success(
      """
        |{
        | group_by_first := \xs, ys -> select x, * from x in xs, y in ys where x = y.age group by x;
        | data := group_by_first(list(1,2,3),students);
        | filter_more := \xs -> select * from x in xs where x.x > 40;
        | filter_more(data)
        |}
      """.stripMargin,
      TestWorlds.professors_students,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(
        AttrType("x", IntType()),
        AttrType("_2", CollectionType(MonoidVariable(), RecordType(Attributes(List(
          AttrType("x", IntType()),
          AttrType("name", StringType()),
          AttrType("age", IntType())
      ))))))))))
  }

  test("lookup attributes") {
    success(
      """select year from (select * from authors)""",
      TestWorlds.publications,
      CollectionType(MonoidVariable(), IntType()))
  }

  test("lookup attributes #2") {
    success(
      """select year, title from (select * from authors)""",
      TestWorlds.publications,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("year", IntType()), AttrType("title", StringType()))))))
  }

  test("field does not exist - issue #4") {
    // TODO: This is a bug: should return UnknownDecl...
    failure(
      """select a.missing_field from authors a""",
      TestWorlds.publications,
      UnexpectedType(UserType(Symbol("Author")), RecordType(AttributesVariable(Set(AttrType("missing_field", TypeVariable()))))))
  }

  test("field does not exist - issue #4 2") {
    // TODO: This is a bug: should return UnknownDecl...
    failure(
      """select missing_field from authors""",
      TestWorlds.publications,
      UnknownDecl(IdnUse("missing_field")))
  }

  test("(1, 2) into (column1: _1, column2: _2)") {
    success(
      """(1, 2) into (column1: _1, column2: _2)""",
      TestWorlds.empty,
      RecordType(Attributes(List(AttrType("column1", IntType()), AttrType("column2", IntType())))))
  }

  test("(number: 1, 2) into (column1: number, column2: _2)") {
    success(
      """(number: 1, 2) into (column1: number, column2: _2)""",
      TestWorlds.empty,
      RecordType(Attributes(List(AttrType("column1", IntType()), AttrType("column2", IntType())))))
  }

  test("""select row parse as r"(\\w+)\\s+" from file row""") {
    success(
      """select row parse as r"(\\w+)\\s+" from file row""",
      TestWorlds.text_file,
      CollectionType(MonoidVariable(), StringType()))
  }

  test("""select row parse as r"(name:\\w+)\\s+" from file row""") {
    success(
      """select row parse as r"(name:\\w+)\\s+" from file row""",
      TestWorlds.text_file,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("name", StringType()))))))
  }

  test("""select row parse as r"(\\w+)\\s+(\\d+)" into (word: _1, n: to_int(_2)) from file row""") {
    success(
      """select row parse as r"(\\w+)\\s(\\d+)" into (word: _1, n: to_int(_2)) from file row""",
      TestWorlds.text_file,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("word", StringType()), AttrType("n", IntType()))))))
  }

  test("invalid regex expression") {
    failure(
      """select row parse as r"(\\w+" from file row""",
      TestWorlds.text_file,
      InvalidRegexSyntax("`)' expected but end of source found"))
  }

  ignore("regex const in expblock") {
    success(
      """
        |{
        |  reg := r"(\\w+).*";
        |  select a.name parse as reg from authors a
        |}
      """.stripMargin,
      TestWorlds.authors_publications,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("reg", StringType())))))
    )
  }

  test("regex with nothing to match") {
    failure(
      """select a.name parse as r"\\w+" from authors a""",
      TestWorlds.authors_publications,
      InvalidRegexSyntax("nothing to match"))
  }

  test("parsing http logs") {
    success(
      "select row parse as r\"\"\"(host:[\\w\\.\\d]+)\\s+-\\s+-\\s+\\[(date:.*)\\]\\s*\"(method:\\w+)\\s+(path:[^\\s]+) (protocol:\\w+)/(version:[0-9.]+)\\s*\"\\s+(returned:\\d+)\\s+(size:\\d+).*\"\"\"\ninto (host:host, file_size: toInt(size) ) from file row",
      TestWorlds.text_file,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("host", StringType()), AttrType("file_size", IntType()))))))
  }

  test("""to_epoch("2015/01/02", "yyyy/MM/dd")""") {
    success(
      """to_epoch("2015/01/02", "yyyy/MM/dd")""",
      TestWorlds.empty,
      IntType())
  }

  test("invalid datetime format string") {
    failure(
      """to_epoch("2015/01/02", "B")""",
      TestWorlds.empty,
      InvalidDateTimeFormatSyntax("Unknown pattern letter: B"))
  }

  test("issue #114") {
    success(
      """
      |{
      |  a := list("foo x", "foo y");
      |  b := list("bar x", "bar y");
      |  full_table := a append b;
      |  select row parse as r"(\\w+)\\s+(\\w+)" from row in full_table
      |}
      """.stripMargin,
      TestWorlds.empty,
      CollectionType(MonoidVariable(), RecordType(Attributes(List(AttrType("_1", StringType()), AttrType("_2", StringType()))))))
  }

  test("fooo1234") {
    success(
      """
        |{
        |  a := \x -> x + x;
        |  b := \y -> a(y);
        |  b(1), b(1.2)
        |}
       """.stripMargin,
      TestWorlds.empty,
      IntType())
  }

}
