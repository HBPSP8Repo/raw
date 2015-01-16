package raw.calculus

import raw._

class SemanticAnalyzerTest extends FunTest {

  def process(w: World, q: String) = w.tipe(parse(w, q))

  test("cern_events") {
    assert(
      process(TestWorlds.cern, "for (e <- Events, e.RunNumber > 100, m <- e.muons) yield set (muon := m)")
        ===
        CollectionType(
          SetMonoid(),
          RecordType(List(
            AttrType("muon", RecordType(List(
              AttrType("pt", FloatType()),
              AttrType("eta", FloatType()))))))))
  }

  test("paper_query_1") {
    assert(
      process(TestWorlds.departments, """for ( el <- for ( d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
        ===
        CollectionType(
          SetMonoid(),
          RecordType(List(
            AttrType("name", StringType()),
            AttrType("address", RecordType(List(
              AttrType("street", StringType()),
              AttrType("zipcode", StringType()))))))))
  }

  test("paper_query_2") {
    assert(
      process(TestWorlds.employees, "for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)")
        ===
        CollectionType(
          SetMonoid(),
          RecordType(List(
            AttrType("E",
              RecordType(List(
                AttrType("children", CollectionType(
                  ListMonoid(),
                  RecordType(List(
                    AttrType("age", IntType()))))),
                AttrType("manager", RecordType(List(
                  AttrType("name", StringType()),
                  AttrType("children", CollectionType(
                    ListMonoid(),
                    RecordType(List(
                      AttrType("age", IntType()))))))))))),
            AttrType("M", IntType())))))
  }
}