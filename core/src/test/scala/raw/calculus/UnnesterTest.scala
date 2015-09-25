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
    val algebra = Unnester(t, w)
    logger.debug(s"algebra\n${CalculusPrettyPrinter(algebra.root)}")
    logger.debug(s"algebra\n${(algebra.root)}")
    val analyzer2 = new calculus.SemanticAnalyzer(algebra, w)
    analyzer2.errors.foreach(err => logger.error(err.toString))
    assert(analyzer2.errors.length === 0)
    compare(TypesPrettyPrinter(analyzer2.tipe(algebra.root)), TypesPrettyPrinter(analyzer.tipe(t.root)))
    algebra
  }

  test("reduce #1") {
    val t = process(
      TestWorlds.departments,
      "for (d <- Departments) yield set d")
    compare(CalculusPrettyPrinter(t.root),
      """
      reduce(set, $0 <- filter($0 <- filter($0 <- Departments, true), true), $0)
      """)
  }

  test("reduce #2") {
    val t = process(
      TestWorlds.departments,
      "for (d <- Departments) yield set d.name")
    compare(CalculusPrettyPrinter(t.root),
      """
      reduce(set, $0 <- filter($0 <- filter($0 <- Departments, true), true), $0.name)
      """)
  }

  test("simple join") {
    val t = process(
      TestWorlds.departments,
      "for (a <- Departments; b <- Departments; a.dno = b.dno) yield set (a1: a, b1: b)")
    compare(CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $1) <- filter(
          ($0, $1) <- join(
            $0 <- filter($0 <- Departments, true),
            $1 <- filter($1 <- Departments, true),
            true and $0.dno = $1.dno),
          true),
        (a1: $0, b1: $1))
      """)
  }

  test("join") {
    val t = process(
      TestWorlds.fines,
      "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        list,
        ($0, $1) <- filter(
          ($0, $1) <- join(
            $0 <- filter($0 <- speed_limits, true),
            $1 <- filter($1 <- radar, true),
            true and $0.location = $1.location and $1.speed > $0.max_speed),
          true),
        (name: $1.person, location: $1.location))
      """)
  }

  test("join 2") {
    val t = process(
      TestWorlds.fines,
      "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        list,
        ($0, $1) <- filter(
          ($0, $1) <- join(
            $0 <- filter($0 <- speed_limits, true),
            $1 <- filter($1 <- radar, true),
            true and $0.location = $1.location and $1.speed < $0.min_speed or $1.speed > $0.max_speed),
          true),
        (name: $1.person, location: $1.location))
    """)
  }

  test("join 3 (split predicates)") {
    val t = process(
      TestWorlds.school,
      """for (s <- students; p <- profs; d <- departments; s.office = p.office; p.name = d.prof) yield list s""")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        list,
        (($0, $1), $2) <- filter(
          (($0, $1), $2) <- join(
            ($0, $1) <- join(
              $0 <- filter($0 <- students, true),
              $1 <- filter($1 <- profs, true),
              true and $0.office = $1.office),
            $2 <-  filter($2 <- departments, true),
            true and $1.name = $2.prof),
          true),
        $0)
      """
    )
  }

  test("join 3 (ANDed predicates)") {
    val t = process(
      TestWorlds.school,
      """for (s <- students; p <- profs; d <- departments; s.office = p.office and p.name = d.prof) yield list s""")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        list,
        (($0, $1), $2) <- filter(
          (($0, $1), $2) <- join(
            ($0, $1) <- join(
              $0 <- filter($0 <- students, true),
              $1 <- filter($1 <- profs, true),
              true and $0.office = $1.office),
            $2 <- filter($2 <- departments, true),
            true and $1.name = $2.prof),
          true),
        $0)
      """
    )
  }

  test("nested comprehension on predicate w/o dependency") {
    val t = process(
      TestWorlds.professors_students,
      """for (s <- students; s.age < for (p <- professors) yield max p.age) yield set s.name"""
    )
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            max,
            ($0, $1) <- outer_join(
              $0 <- filter($0 <- students, true),
              $1 <- filter($1 <- professors, true),
              true),
            $0,
            true,
            $1.age),
          true and $0.age < $2),
        $0.name)
      """
    )
  }

  test("nested comprehension on predicate w/ dependency") {
    val t = process(
      TestWorlds.professors_students,
      """
      for (s <- students;
           s.age = (for (p <- professors; p.age = s.age) yield sum 1)
           )
        yield set s.name
      """
    )
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            sum,
            ($0, $1) <- outer_join(
              $0 <- filter($0 <- students, true),
              $1 <- filter($1 <- professors, true),
              true and $1.age = $0.age),
            $0,
            true,
            1),
          true and $0.age = $2),
        $0.name)
      """)
  }

  test("nested comprehension on predicate w/ and w/o dependency") {
    val t = process(
      TestWorlds.professors_students,
      """
      for (s <- students;
           s.age < (for (p <- professors) yield max p.age);
           s.age = (for (s <- students; s.age = (for (p <- professors) yield sum 1)) yield max s.age))
        yield set s.name
      """
    )
    compare(
      CalculusPrettyPrinter(t.root),
    """
    reduce(
      set,
      (($0, $4), $5) <- filter(
        (($0, $4), $5) <- nest(
          max,
          ((($0, $4), $6), $2) <- outer_join(
            (($0, $4), $6) <- nest(
              sum,
              (($0, $4), $3) <- outer_join(
                ($0, $4) <- nest(
                  max,
                  ($0, $1) <- outer_join(
                    $0 <- filter($0 <- students, true),
                    $1 <- filter($1 <- professors, true),
                    true),
                  $0,
                  true,
                  $1.age),
                $3 <- filter($3 <- professors, true),
                true),
              (_1: $0, _2: $4),
              true,
              1),
            $2 <- filter($2 <- students, true),
            true and $2.age = $6),
          (_1: $0, _2: $4),
          true,
          $2.age),
        true and $0.age < $4 and $0.age = $5),
      $0.name)
    """
    )
  }

  // TODO: Add test cases with nested non-primitive reduces!

    test("paper query A") {
      val t = process(
        TestWorlds.employees,
        "for (d <- Departments) yield set (D: d, E: for (e <- Employees; e.dno = d.dno) yield set e)")
      compare(
        CalculusPrettyPrinter(t.root),
        """
      reduce(
        set,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            set,
            ($0, $1) <- outer_join(
              $0 <- filter($0 <- Departments, true),
              $1 <- filter($1 <- Employees, true),
              true and $1.dno = $0.dno),
            $0,
            true,
            $1),
          true),
        (D: $0, E: $2))
      """
    )
  }

  test("paper query A variation") {
    val t = process(
      TestWorlds.employees,
      "for (d <- Departments) yield set (D: d, E: for (e <- Employees; e.dno = d.dno) yield set e.dno)")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            set,
            ($0, $1) <- outer_join(
              $0 <- filter($0 <- Departments, true),
              $1 <- filter($1 <- Employees, true),
              true and $1.dno = $0.dno),
            $0,
            true,
            $1.dno),
          true),
        (D: $0, E: $2))
      """
    )
  }

  test("paper query B") {
    val t = process(
      TestWorlds.A_B,
      "for (a <- A) yield and (for (b <- B; a = b) yield or true)")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        and,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            or,
            ($0, $1) <- outer_join(
              $0 <- filter($0 <- A, true),
              $1 <- filter($1 <- B, true),
              true and $0 = $1),
            $0,
            true,
            true),
          true),
        $2)
      """
    )
  }

  test("paper query C") {
    val t = process(
      TestWorlds.employees,
      "for (e <- Employees) yield set (E: e, M: for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $3) <- filter(
          ($0, $3) <- nest(
            sum,
            (($0, $1), $4) <- nest(
              and,
              (($0, $1), $2) <- outer_unnest(
                ($0, $1) <- outer_unnest(
                  $0 <- filter($0 <- Employees, true),
                  $1 <- $0.children,
                  true),
                $2 <- $0.manager.children,
                true),
              (_1: $0, _2: $1),
              true,
              $1.age > $2.age),
            $0,
            true and $4,
            1),
          true),
        (E: $0, M: $3))
      """
    )
  }

  test("paper query D") {
    val t = process(
      TestWorlds.transcripts,
      """for (s <- Students; for (c <- Courses; c.title = "DB") yield and (for (t <- Transcripts; t.id = s.id; t.cno = c.cno) yield or true)) yield set s""")
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(
        set,
        ($0, $3) <- filter(
          ($0, $3) <- nest(
            and,
            (($0, $1), $4) <- nest(
              or,
              (($0, $1), $2) <- outer_join(
                ($0, $1) <- outer_join(
                  $0 <- filter($0 <- Students, true),
                  $1 <- filter($1 <- Courses, true and $1.title = "DB"),
                  true),
                $2 <- filter($2 <- Transcripts, true),
                true and $2.id = $0.id and $2.cno = $1.cno),
              (_1: $0, _2: $1),
              true,
              true),
            $0,
            true,
            $4),
          true and $3),
        $0)
      """
    )
  }

  test("top-level merge") {
    val t = process(
      TestWorlds.things,
      "for (x <- things union things) yield set x"
    )
    compare(
      CalculusPrettyPrinter(t.root),
      """
      reduce(set, $0 <- filter($0 <- filter($0 <- things, true), true), $0)
        union
      reduce(set, $1 <- filter($1 <- filter($1 <- things, true), true), $1)"""
    )
  }

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