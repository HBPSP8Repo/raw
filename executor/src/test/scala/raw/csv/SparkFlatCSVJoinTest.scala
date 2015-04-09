package raw.csv

import raw.Raw
import shapeless.HList

class SparkFlatCSVJoinTest extends AbstractSparkFlatCSVTest {
//  test("cross product professors x departments x students") {
//    // How can we cast to a Set[X] where X is a class known by the client API?
//    val q: Set[Any] = Raw.query(
//      "for (p <- profs; d <- departments; s <- students) yield set (professor := p, dept := d, student := s)",
//      HList("profs" -> profs, "students" -> students, "departments" -> departments)).asInstanceOf[Set[Any]]
////    printQueryResult(q)
//    assert(q.size === 3 * 3 * 7)
//  }
//
//  test("cross product professors x departments") {
//    val q: Set[Any] = Raw.query(
//      "for (p <- profs; d <- departments) yield set (p.name, p.office, d.name, d.discipline, d.prof)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
////    printQueryResult(q)
//    assert(q.size === 9)
//  }
//
//  test("inner join professors x departments") {
//    val q: Set[Any] = Raw.query(
//      "for (p <- profs; d <- departments; p.name = d.prof) " +
//        "yield set (name := p.name, officeName := p.office, deptName := d.name, discipline := d.discipline)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
////    printQueryResult(q)
//    assert(q.size === 3)
//  }
//
//  test("Spark JOIN: (professors, departments) with predicate") {
//    val q: Set[Any] = Raw.query(
//      "for (p <- profs; d <- departments; d.name.last = p.name.last) " +
//        "yield set (p.name, p.office, d.name, d.discipline, d.prof)",
//      HList("profs" -> profs, "departments" -> departments)).asInstanceOf[Set[Any]]
////    printQueryResult(q)
//    assert(q.size === 3)
//  }

//  test("Spark JOIN: (professors, departments, students) with one predicate per datasource") {
//    val q: Set[Any] = Raw.query(
//      """for (p <- profs; d <- departments; s <- students;
//                p.name <> "Dr, Who"; d.name <> "Magic University"; s.name <> "Batman")
//          yield set (p.name, p.office, d.name, d.discipline, d.prof, s.name)""",
//      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
//    printQueryResult(q)
//  }
//
//  test("Spark JOIN: (professors, departments, students) predicate with triple condition") {
//    val q: Set[Any] = Raw.query(
//      "for (p <- profs; d <- departments; s <- students; " +
//        "d.name <> p.name and p.name <> s.name and s.name <> d.name) " +
//        "yield set (p.name, d.name, s.name)",
//      HList("profs" -> profs, "departments" -> departments, "students" -> students)).asInstanceOf[Set[Any]]
//    printQueryResult(q)
//  }

  def printQueryResult(res: Set[Any]) = {
    val str = res.map(r => r.asInstanceOf[ {def toMap(): Map[Any, Any]}].toMap()).mkString("\n")
    println("Result:\n" + str)
  }
}
