package raw
package calculus

class UnnesterTest extends CalculusTest {

  val phase = "Unnester"

  test("reduce #1") {
    check(
      "for (d <- Departments) yield set d",
      """
      reduce(set, d$0 <- filter(d$0 <- filter(d$0 <- Departments, true), true), d$0)
      """,
      TestWorlds.departments)
  }

  test("reduce #2") {
    check(
      "for (d <- Departments) yield set d.name",
      """
      reduce(set, d$0 <- filter(d$0 <- filter(d$0 <- Departments, true), true), d$0.name)
      """,
      TestWorlds.departments)
  }

  test("simple join") {
    check(
      "for (a <- Departments; b <- Departments; a.dno = b.dno) yield set (a1: a, b1: b)",
      """
      reduce(
        set,
        (a$0, b$0) <- filter(
          (a$0, b$0) <- join(
            a$0 <- filter(a$0 <- Departments, true),
            b$0 <- filter(b$0 <- Departments, true),
            a$0.dno = b$0.dno),
          true),
        (a1: a$0, b1: b$0))
      """,
      TestWorlds.departments)
  }

  test("join") {
    check(
      "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)",
      """
      reduce(
        list,
        (speed_limit$0, observation$0) <- filter(
          (speed_limit$0, observation$0) <- join(
            speed_limit$0 <- filter(speed_limit$0 <- speed_limits, true),
            observation$0 <- filter(observation$0 <- radar, true),
            speed_limit$0.location = observation$0.location and observation$0.speed > speed_limit$0.max_speed),
          true),
        (name: observation$0.person, location: observation$0.location))
      """,
      TestWorlds.fines)
  }

  test("join 2") {
    check(
      "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name: observation.person, location: observation.location)",
      """
      reduce(
        list,
        (speed_limit$0, observation$0) <- filter(
          (speed_limit$0, observation$0) <- join(
            speed_limit$0 <- filter(speed_limit$0 <- speed_limits, true),
            observation$0 <- filter(observation$0 <- radar, true),
            speed_limit$0.location = observation$0.location and observation$0.speed < speed_limit$0.min_speed or observation$0.speed > speed_limit$0.max_speed),
          true),
        (name: observation$0.person, location: observation$0.location))
      """,
      TestWorlds.fines)
  }

  test("join 3 (split predicates)") {
    check(
      """for (s <- students; p <- profs; d <- departments; s.office = p.office; p.name = d.prof) yield list s""",
      """
      reduce(
        list,
        ((s$0, p$0), d$0) <- filter(
          ((s$0, p$0), d$0) <- join(
            (s$0, p$0) <- join(
              s$0 <- filter(s$0 <- students, true),
              p$0 <- filter(p$0 <- profs, true),
              s$0.office = p$0.office),
            d$0 <-  filter(d$0 <- departments, true),
            p$0.name = d$0.prof),
          true),
        s$0)
      """,
      TestWorlds.school)
  }

  test("join 3 (ANDed predicates)") {
    check(
      """for (s <- students; p <- profs; d <- departments; s.office = p.office and p.name = d.prof) yield list s""",
      """
      reduce(
        list,
        ((s$0, p$0), d$0) <- filter(
          ((s$0, p$0), d$0) <- join(
            (s$0, p$0) <- join(
              s$0 <- filter(s$0 <- students, true),
              p$0 <- filter(p$0 <- profs, true),
              s$0.office = p$0.office),
            d$0 <- filter(d$0 <- departments, true),
            p$0.name = d$0.prof),
          true),
        s$0)
      """,
      TestWorlds.school)
  }

  test("nested comprehension on predicate w/o dependency") {
    check(
      """for (s <- students; s.age < for (p <- professors) yield max p.age) yield set s.name""",
      """
      reduce(
        set,
        (s$0, $1) <- filter(
          (s$0, $1) <- nest(
            max,
            (s$0, p$0) <- outer_join(
              s$0 <- filter(s$0 <- students, true),
              p$0 <- filter(p$0 <- professors, true),
              true),
            s$0,
            true,
            p$0.age),
          s$0.age < $1),
        s$0.name)
      """,
      TestWorlds.professors_students)
  }

  test("nested comprehension on predicate w/ dependency") {
    check(
      """
      for (s <- students;
           s.age = (for (p <- professors; p.age = s.age) yield sum 1)
           )
        yield set s.name
      """,
      """
      reduce(
        set,
        (s$0, $1) <- filter(
          (s$0, $1) <- nest(
            sum,
            (s$0, p$0) <- outer_join(
              s$0 <- filter(s$0 <- students, true),
              p$0 <- filter(p$0 <- professors, true),
              p$0.age = s$0.age),
            s$0,
            true,
            1),
          s$0.age = $1),
        s$0.name)
      """,
      TestWorlds.professors_students)
  }

  test("nested comprehension on predicate w/ and w/o dependency") {
    check(
      """
      for (s <- students;
           s.age < (for (p <- professors) yield max p.age);
           s.age = (for (s <- students; s.age = (for (p <- professors) yield sum 1)) yield max s.age))
        yield set s.name
      """,
      """
      reduce(
        set,
        ((s$0, $4), $5) <- filter(
          ((s$0, $4), $5) <- nest(
            max,
            (((s$0, $4), $6), s$1) <- outer_join(
              ((s$0, $4), $6) <- nest(
                sum,
                ((s$0, $4), p$1) <- outer_join(
                  (s$0, $4) <- nest(
                    max,
                    (s$0, p$0) <- outer_join(
                      s$0 <- filter(s$0 <- students, true),
                      p$0 <- filter(p$0 <- professors, true),
                      true),
                    s$0,
                    true,
                    p$0.age),
                  p$1 <- filter(p$1 <- professors, true),
                  true),
                (_1: s$0, _2: $4),
                true,
                1),
              s$1 <- filter(s$1 <- students, true),
              s$1.age = $6),
            (_1: s$0, _2: $4),
            true,
            s$1.age),
          s$0.age < $4 and s$0.age = $5),
        s$0.name)
      """,
      TestWorlds.professors_students)
  }

  // TODO: Add test cases with nested non-primitive reduces!

  test("paper query A") {
    check(
      "for (d <- Departments) yield set (D: d, E: for (e <- Employees; e.dno = d.dno) yield set e)",
      """
      reduce(
        set,
        (d$0, $2) <- filter(
          (d$0, $2) <- nest(
            set,
            (d$0, e$0) <- outer_join(
              d$0 <- filter(d$0 <- Departments, true),
              e$0 <- filter(e$0 <- Employees, true),
              e$0.dno = d$0.dno),
            d$0,
            true,
            e$0),
          true),
        (D: d$0, E: $2))
      """,
      TestWorlds.employees)
  }

  test("paper query A variation") {
    check(
      "for (d <- Departments) yield set (D: d, E: for (e <- Employees; e.dno = d.dno) yield set e.dno)",
      """
      reduce(
        set,
        (d$0, $2) <- filter(
          (d$0, $2) <- nest(
            set,
            (d$0, e$0) <- outer_join(
              d$0 <- filter(d$0 <- Departments, true),
              e$0 <- filter(e$0 <- Employees, true),
              e$0.dno = d$0.dno),
            d$0,
            true,
            e$0.dno),
          true),
        (D: d$0, E: $2))
      """,
      TestWorlds.employees)
  }

  test("paper query B") {
    check(
      "for (a <- A) yield and (for (b <- B; a = b) yield or true)",
      """
      reduce(
        and,
        (a$0, $2) <- filter(
          (a$0, $2) <- nest(
            or,
            (a$0, b$0) <- outer_join(
              a$0 <- filter(a$0 <- A, true),
              b$0 <- filter(b$0 <- B, true),
              a$0 = b$0),
            a$0,
            true,
            true),
          true),
        $2)
      """,
      TestWorlds.A_B)
  }

  test("paper query C") {
    check(
      "for (e <- Employees) yield set (E: e, M: for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)",
      """
      reduce(
        set,
        (e$0, $3) <- filter(
          (e$0, $3) <- nest(
            sum,
            ((e$0, c$0), $4) <- nest(
              and,
              ((e$0, c$0), d$0) <- outer_unnest(
                (e$0, c$0) <- outer_unnest(
                  e$0 <- filter(e$0 <- Employees, true),
                  c$0 <- e$0.children,
                  true),
                d$0 <- e$0.manager.children,
                true),
              (_1: e$0, _2: c$0),
              true,
              c$0.age > d$0.age),
            e$0,
            $4,
            1),
          true),
        (E: e$0, M: $3))
      """,
      TestWorlds.employees)
  }

  test("paper query D") {
    check(
      """
      for (s <- Students; for (c <- Courses; c.title = "DB") yield and (for (t <- Transcripts; t.id = s.id; t.cno = c.cno) yield or true)) yield set s
      """,
      """
      reduce(
        set,
        (s$0, $3) <- filter(
          (s$0, $3) <- nest(
            and,
            ((s$0, c$0), $4) <- nest(
              or,
              ((s$0, c$0), t$0) <- outer_join(
                (s$0, c$0) <- outer_join(
                  s$0 <- filter(s$0 <- Students, true),
                  c$0 <- filter(c$0 <- Courses, c$0.title = "DB"),
                  true),
                t$0 <- filter(t$0 <- Transcripts, true),
                t$0.id = s$0.id and t$0.cno = c$0.cno),
              (_1: s$0, _2: c$0),
              true,
              true),
            s$0,
            true,
            $4),
          $3),
        s$0)
      """,
      TestWorlds.transcripts)
  }

  test("top-level merge") {
    check(
      "for (x <- things union things) yield set x",
      """
      reduce(set, x$0 <- filter(x$0 <- filter(x$0 <- things, true), true), x$0)
        union
      reduce(set, x$1 <- filter(x$1 <- filter(x$1 <- things, true), true), x$1)""",
      TestWorlds.things)
  }

  test("publications: count of authors grouped by title and then year") {
    check(
      "select distinct title: A.title, stats: (select year: A.year, N: count(partition) from A in partition group by A.year) from A in authors group by A.title",
      """
      """,
      TestWorlds.publications)
  }

  ignore("nested nests") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/nest/outer-join/nest/outer-join....
    check(
      "select distinct A.title, sum(select a.year from a in partition), count(partition), max(select a.year from a in partition) from A in authors group by A.title",
      "",
      TestWorlds.publications)
  }

  ignore("nested nests 2") {
    // to see the shape of nest/outer-join chains, this leads to outer-join/outer-join/outer-join/..../nest/nest/nest/....
    check(
      "select distinct A.title, (select distinct A.year, (select distinct A.name, count(partition) from partition A group by A.name) from A in partition group by A.year) from A in authors group by A.title",
      "",
      TestWorlds.publications)
  }

  test("group by w/ expression") {
    check(
      """select distinct year/100, * from authors group by year/100""",
      """
      reduce(
        set,
        ($0, $2) <- filter(
          ($0, $2) <- nest(
            bag,
            ($0, $1) <- outer_join($0 <- filter($0 <- authors, true), $1 <- filter($1 <- authors, true), $0.year / 100 = $1.year / 100),
            $0,
            true,
            (name: $1.name, title: $1.title, year: $1.year)),
          true),
        (_1: $0.year / 100, _2: $2))
      """.stripMargin,
      TestWorlds.publications,
      ignoreRootTypeComparison = true)
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