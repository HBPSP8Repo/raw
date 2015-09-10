//package raw
//package algebra
//
//import raw.calculus.{Simplifier, Calculus}
//
//class TyperTest extends FunTest {
//
//  def assertRootType(query: String, issue: String = "", world: World = null) = {
//    val run = (d: String) => if (issue != "") ignore(d + s" ($issue)") _ else test(d) _
//    run(query) {
//      val w: World =
//        if (world != null)
//          world
//        else
//          new World(sources=Map(
//            "unknown" -> SetType(AnyType()),
//            "integers" -> SetType(IntType()),
//            "floats" -> SetType(FloatType()),
//            "booleans" -> SetType(BoolType()),
//            "strings" -> SetType(StringType()),
//            "records" -> SetType(RecordType(scala.collection.immutable.Seq(AttrType("i", IntType()), AttrType("f", FloatType())), None)),
//            "unknownrecords" -> SetType(RecordType(scala.collection.immutable.Seq(AttrType("dead", AnyType()), AttrType("alive", AnyType())), None))))
//
//      val ast = parse(query)
//      val ct = new Calculus.Calculus(ast)
//
//      val calculusAnalyzer = new calculus.SemanticAnalyzer(ct, w)
//      calculusAnalyzer.errors.foreach(err => logger.error(err.toString))
//
//      val ast1 = Unnester(ct, w)
//      val at = new LogicalAlgebra.Algebra(ast1)
//
//      val algebraTyper = new Typer(w)
//
//      assert(calculusAnalyzer.tipe(ast) === algebraTyper.tipe(ast1))
//    }
//  }
//
//  assertRootType(
//    "for (e <- Events; e.RunNumber > 100; m <- e.muons) yield set (muon := m)",
//    world=TestWorlds.cern)
//
//  assertRootType(
//    """for ( el <- for ( d <- Departments; d.name = "CSE") yield set d.instructors; e <- el; for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""",
//    world=TestWorlds.departments)
//
//  assertRootType(
//    "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)",
//    world=TestWorlds.employees)
//
//  assertRootType(
//    """for ( d <- Departments; d.name = "CSE") yield set { name := d.name; (deptName := name) }""",
//    world=TestWorlds.departments)
//
//  assertRootType(
//    """for (d <- Departments; d.dno > for (i <- d.instructors; deg <- i.degrees ) yield sum 1) yield set d """,
//    world=TestWorlds.departments)
//
//// TODO: The following tests require the source types to be inferred and then to have those inferred types used in the algebra.
////  assertRootType("for (r <- integers) yield set r + 1")
////  assertRootType("for (r <- unknown) yield set r + 1")
////  assertRootType("for (r <- unknown) yield set r + 1.0")
////  assertRootType("for (r <- unknown; x <- integers; r = x) yield set r")
////  assertRootType("for (r <- unknown; x <- integers; r + x = 2*x) yield set r")
////  assertRootType("for (r <- unknown; x <- floats; r + x = x) yield set r")
////  assertRootType("for (r <- unknown; x <- booleans; r and x) yield set r")
////  assertRootType("for (r <- unknown; x <- strings; r = x) yield set r")
////  assertRootType("for (r <- unknown) yield max (r + (for (i <- integers) yield max i))")
////  assertRootType("for (r <- unknown; ((r.age + r.birth) > 2015) = r.alive) yield set r", "data source record type is not inferred")
////  assertRootType("for (r <- unknown; (for (x <- integers) yield and r > x) = true) yield set r")
////  assertRootType("for (r <- unknown; (for (x <- records) yield set (r.value > x.f)) = true) yield set r", "missing record type inference")
////  assertRootType("""for (r <- unknown; f := (\v -> v + 2)) yield set f(r)""")
////  assertRootType("for (r <- unknown; v := r) yield set (r + 0)")
////  assertRootType("for (r <- unknownrecords) yield set r.dead or r.alive")
////  assertRootType("for (r <- unknownrecords; r.dead or r.alive) yield set r", "data source record type is not inferred")
////  assertRootType("for (r <- integers; (a,b) := (1, 2)) yield set (a+b)")
//
//  assertRootType(
//    """
//    for (c <- diagnosis_codes) yield list {
//      code := c.diagnostic_code;
//      desc := c.description;
//
//      patient_ids := for (d <- diagnosis; d.diagnostic_code = code) yield list d.patient_id;
//
//      count := for (p <- patient_ids) yield sum 1;
//
//      others :=
//      for (p <- patient_ids;
//           d <- diagnosis;
//           c <- diagnosis_codes;
//           d.diagnostic_code = c.diagnostic_code;
//           d.diagnostic_code <> code;
//           d.patient_id = p)
//        yield list (c.description);
//
//      unique_others := for (o <- others) yield set o;
//
//      desc_count := for (o1 <- others) yield list (desc := o1, count := for (o2 <- others; o1 = o2) yield sum 1);
//
//      (desc := desc, others := desc_count)
//    }
//    """, world=TestWorlds.chuv_diagnosis
//  )
//}