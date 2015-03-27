package raw
package algebra

import calculus.{Calculus, Simplifier}

class UnnesterTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val t = new Calculus.Calculus(ast)
    val analyzer = new calculus.SemanticAnalyzer { val tree = t; val world = w }
    assert(analyzer.errors.length === 0)
    Unnester(t, w)
  }

  test("simple join") {
    val query = "for (a <- Departments; b <- Departments; a.dno = b.dno) yield set (a1 := a, b1 := b)"
    val w = TestWorlds.departments

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          set,
          record("a1" -> arg._1, "b1" -> arg._2),
          join(
            arg._1.dno == arg._2.dno,
            select(scan("Departments")),
            select(scan("Departments"))))
      }
    }

    assert(process(w, query) === Result())
  }

  test("join") {
    val query = "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"
    val w = TestWorlds.fines

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          list,
          record("name" -> arg._2.person, "location" -> arg._2.location),
          join(
            arg._1.location == arg._2.location && arg._2.speed > arg._1.max_speed,
            select(scan("speed_limits")),
            select(scan("radar"))))
      }
    }

    assert(process(w, query) === Result())
  }

  test("join 2") {
    val query = "for (speed_limit <- speed_limits; observation <- radar; speed_limit.location = observation.location; observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.person, location := observation.location)"
    val w = TestWorlds.fines

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          list,
          record("name" -> arg._2.person, "location" -> arg._2.location),
          true,
          join(
            (arg._1.location == arg._2.location) && ((arg._2.speed < arg._1.min_speed) || arg._2.speed > arg._1.max_speed),
            select(true, scan("speed_limits")),
            select(true, scan("radar"))))
      }
    }

    assert(process(w, query) === Result())
  }

  test("join 3 (split predicates)") {
    val query = """for (s <- students; p <- profs; d <- departments; s.office = p.office; p.name = d.prof) yield list s"""
    val w = TestWorlds.school

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          list,
          arg._1._1,
          true,
          join(
            arg._1._2.name == arg._2.prof,
            join(
              arg._1.office == arg._2.office,
              select(true, scan("students")),
              select(true, scan("profs"))),
            select(true, scan("departments"))))
      }
    }

    assert(process(w, query) === Result())
  }

  ignore("join 3 (ANDed predicates)") {
    val query = """for (s <- students; p <- profs; d <- departments; s.office = p.office and p.name = d.prof) yield list s"""
    val w = TestWorlds.school

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          list,
          arg._1._1,
          true,
          join(
            arg._1._2.name == arg._2.prof,
            join(
              arg._1.office == arg._2.office,
              select(true, scan("students")),
              select(true, scan("profs"))),
            select(true, scan("departments"))))
      }
    }

    assert(process(w, query) === Result())
  }

  test("paper query") {
    val query = "for (e <- Employees) yield set (E := e, M := for (c <- e.children; for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)"
    val w = TestWorlds.employees

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        reduce(
          set,
          record("E" -> arg._1, "M" -> arg._2),
          nest(
            sum,
            e=1,
            group_by=arg._1._1,
            p=arg._2,
            nulls=record("_1" -> arg._1._2, "_2" -> arg._2),
            nest(
              and,
              e=arg._1._2.age > arg._2.age,
              group_by=record("_1" -> arg._1._1, "_2" -> arg._1._2),
              nulls=arg._2,
              outer_unnest(
                path=arg._1.manager.children,
                outer_unnest(
                  path=arg.children,
                  select(
                    scan("Employees")))))))
      }
    }

    logger.error(LogicalAlgebraPrettyPrinter(Result()))
    logger.error(LogicalAlgebraPrettyPrinter(process(w, query)))
    assert(process(w, query) === Result())
  }

  test("top-level merge") {
    val query = "for (x <- things union things) yield set x"
    val w = TestWorlds.things

    object Result extends AlgebraDSL {
      val world = w

      def apply() = {
        merge(
          set,
          reduce(
            set,
            arg,
            select(
              scan("things"))),
          reduce(
            set,
            arg,
            select(
              scan("things"))))
      }
    }
    assert(process(w, query) === Result())
  }

}