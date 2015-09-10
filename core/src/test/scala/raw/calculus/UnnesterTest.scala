package raw
package calculus

class UnnesterTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new calculus.SemanticAnalyzer(t, w)
    logger.debug(calculus.CalculusPrettyPrinter(ast))
    analyzer.errors.foreach(err => logger.error(err.toString))
    assert(analyzer.errors.length === 0)
    Unnester(t, w)
  }

  test("simple join") {
    val t = process(TestWorlds.departments, "for (a <- Departments; b <- Departments; a.dno = b.dno) yield set (a1 := a, b1 := b)")
    compare(CalculusPrettyPrinter(t.root),
      """
        reduce(
            set,
            \($0, $1) -> (a1 := $0, b1 := $1),
            \($0, $1) -> true,
             join(
                    \($0, $1) -> true and $0.dno = $1.dno,
                     filter(\$0 -> true, Departments),
                     filter(\$1 -> true, Departments)))

      """)
  }
  
//
//  test("join") {
//    val query = "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"
//    val w = TestWorlds.fines
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          list,
//          record("name" -> arg._2.person, "location" -> arg._2.location),
//          join(
//            arg._1.location == arg._2.location && arg._2.speed > arg._1.max_speed,
//            select(scan("speed_limits")),
//            select(scan("radar"))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("join 2") {
//    val query = "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"
//    val w = TestWorlds.fines
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          list,
//          record("name" -> arg._2.person, "location" -> arg._2.location),
//          true,
//          join(
//            (arg._1.location == arg._2.location) && ((arg._2.speed < arg._1.min_speed) || arg._2.speed > arg._1.max_speed),
//            select(true, scan("speed_limits")),
//            select(true, scan("radar"))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("join 3 (split predicates)") {
//    val query = """for (s <- students; p <- profs; d <- departments; s.office = p.office; p.name = d.prof) yield list s"""
//    val w = TestWorlds.school
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          list,
//          arg._1._1,
//          true,
//          join(
//            arg._1._2.name == arg._2.prof,
//            join(
//              arg._1.office == arg._2.office,
//              select(true, scan("students")),
//              select(true, scan("profs"))),
//            select(true, scan("departments"))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("join 3 (ANDed predicates)") {
//    val query = """for (s <- students; p <- profs; d <- departments; s.office = p.office and p.name = d.prof) yield list s"""
//    val w = TestWorlds.school
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          list,
//          arg._1._1,
//          true,
//          join(
//            arg._1._2.name == arg._2.prof,
//            join(
//              arg._1.office == arg._2.office,
//              select(true, scan("students")),
//              select(true, scan("profs"))),
//            select(true, scan("departments"))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("paper query A") {
//    val query = "for (d <- Departments) yield set (D := d, E := for (e <- Employees; e.dno = d.dno) yield set e)"
//    val w = TestWorlds.employees
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          set,
//          record("D" -> arg._1, "E" -> arg._2),
//          true,
//          nest(
//            set,
//            arg._2,
//            arg._1,
//            true,
//            arg._2,
//            outer_join(
//              arg._2.dno == arg._1.dno,
//              select(true, scan("Departments")),
//              select(true, scan("Employees")))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("paper query B") {
//    val query = "for (a <- A) yield and (for (b <- B; a = b) yield or true)"
//    val w = TestWorlds.A_B
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          and,
//          arg._2,
//          true,
//          nest(
//            or,
//            true,
//            arg._1,
//            true,
//            arg._2,
//            outer_join(
//              arg._1 == arg._2,
//              select(true, scan("A")),
//              select(true, scan("B")))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("paper query C") {
//    val query = "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)"
//    val w = TestWorlds.employees
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          set,
//          record("E" -> arg._1, "M" -> arg._2),
//          nest(
//            sum,
//            e=1,
//            group_by=arg._1._1,
//            p=arg._2,
//            nulls=record("_1" -> arg._1._2, "_2" -> arg._2),
//            nest(
//              and,
//              e=arg._1._2.age > arg._2.age,
//              group_by=record("_1" -> arg._1._1, "_2" -> arg._1._2),
//              nulls=arg._2,
//              outer_unnest(
//                path=arg._1.manager.children,
//                outer_unnest(
//                  path=arg.children,
//                  select(
//                    scan("Employees")))))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("paper query D") {
//    val query = """for (s <- Students; for (c <- Courses; c.title = "DB") yield and (for (t <- Transcripts; t.id = s.id; t.cno = c.cno) yield or true)) yield set s"""
//    val w = TestWorlds.transcripts
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        reduce(
//          set,
//          arg._1,
//          arg._2,
//          nest(
//            and,
//            arg._2,
//            arg._1._1,
//            true,
//            record("_1" -> arg._1._2, "_2" -> arg._2),
//            nest(
//              or,
//              true,
//              record("_1" -> arg._1._1, "_2" -> arg._1._2),
//              true,
//              arg._2,
//              outer_join(
//                arg._2.id == arg._1._1.id && arg._2.cno == arg._1._2.cno,
//                outer_join(
//                  true,
//                  select(scan("Students")),
//                  select(
//                    arg.title == const("DB"),
//                    scan("Courses"))),
//                select(true, scan("Transcripts"))))))
//      }
//    }
//
//    assert(process(w, query) === Result())
//  }
//
//  test("top-level merge") {
//    val query = "for (x <- things union things) yield set x"
//    val w = TestWorlds.things
//
//    object Result extends AlgebraDSL {
//      val world = w
//
//      def apply() = {
//        merge(
//          set,
//          reduce(
//            set,
//            arg,
//            select(
//              scan("things"))),
//          reduce(
//            set,
//            arg,
//            select(
//              scan("things"))))
//      }
//    }
//    assert(process(w, query) === Result())
//  }
//
//  ignore("edge bundling chuv diagnossis") {
//        val query = """
//        for (c <- diagnosis_codes) yield list {
//          code := c.diagnostic_code;
//          desc := c.description;
//
//          patient_ids := for (d <- diagnosis; d.diagnostic_code = code) yield list d.patient_id;
//
//          count := for (p <- patient_ids) yield sum 1;
//
//          others :=
//          for (p <- patient_ids;
//          d <- diagnosis;
//          c <- diagnosis_codes;
//          d.diagnostic_code = c.diagnostic_code;
//          d.diagnostic_code <> code;
//          d.patient_id = p)
//          yield list (c.description);
//
//          unique_others := for (o <- others) yield set o;
//
//          desc_count := for (o1 <- others) yield list (desc := o1, count := for (o2 <- others; o1 = o2) yield sum 1);
//
//          (desc := desc, others := desc_count)
//        }
//        """
//
//    //    val query = """
////    for (c <- diagnosis_codes) yield list {
////      code := c.diagnostic_code;
////      desc := c.description;
////
////      // List of patients that have this disease
////      patient_ids := for (d <- diagnosis, d.diagnostic_code = code) yield list d.patient_id
////
////      // Number of patients that have this disease
////      count := for (p <- patients_ids) yield sum 1 // New syntactic sugar
////      //count := count(patients_ids) // New syntactic sugar
////
////      // Other diseases that these patients have
////      others :=
////      for (p <- patient_ids,
////      d <- diagnosis,
////      c <- diagnostic_codes,
////      d.diagnostic_code = c.diagnostic_code,
////      d.diagnostic_code != code,
////      d.patient_id := p)
////      yield list (c.description)
////
////      unique_others := for (o <- others) yield set o
////      //unique_others := unique(others) // New syntactic sugar equiv to above
////
////      desc_count := for (o1 <- others) yield list (desc := o1, count := for (o2 < others, o1 = o2) yield sum 1)
////      //desc_count := for (o1 <- unique(others) yield list (desc := o1, count := count(o2 <- others, o1 = o2))
////
////      (desc := desc, others := desc_count)
////    }
////    """
////    val query = """
////    for (c <- diagnosis_codes) yield list {
////      code := c.diagnostic_code;
////      desc := c.description;
////
////      patient_ids := for (d <- diagnosis; d.diagnostic_code = code) yield list d.patient_id;
////      for (p <- patients_ids) yield sum 1
////    }
////    """
//    //for (p <- patients_ids) yield sum 1;
////    val q = """
////        for (c <- diagnosis_codes) yield list {
////          code := c.diagnostic_code;
////          desc := c.description;
////          patient_ids := for (d <- diagnosis; d.diagnostic_code = code) yield list d.patient_id;
////          count := for (p <- patient_ids) yield sum 1;
////          others := for (
////            p <- patient_ids;
////            d <- diagnosis;
////            c <- diagnosis_codes;
////            d.diagnostic_code = c.diagnostic_code;
////            d.diagnostic_code <> code;
////            d.patient_id = p) yield list c.description;
////          unique_others := for (o <- others) yield set o;
////          desc_count := for (o1 <- others) yield list (desc := o1, count := for (o2 < others; o1 = o2) yield sum 1);
////          (desc := desc, others := desc_count) }
////        """
//    val w = TestWorlds.chuv_diagnosis
//
//    logger.debug(LogicalAlgebraPrettyPrinter(process(w, query)))
//    assert(false)
//  }
}