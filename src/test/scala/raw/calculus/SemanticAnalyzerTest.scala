package raw.calculus

import raw._

class SemanticAnalyzerTest extends FunTest {

  def process(w: World, q: String) = {
    val ast = parse(q)
    val analyzer = new SemanticAnalyzer(new Calculus.Calculus(ast), w)
    assert(analyzer.errors.length === 0)

    analyzer.debugTreeTypes

    analyzer.tipe(ast)
  }

  test("cern_events") {
    assert(
      process(TestWorlds.cern, "for (e <- Events, e.RunNumber > 100, m <- e.muons) yield set (muon := m)")
        ===
        SetType(
          RecordType(List(
            AttrType("muon", RecordType(List(
              AttrType("pt", FloatType()),
              AttrType("eta", FloatType()))))))))
  }

  test("paper_query_1") {
    assert(
      process(TestWorlds.departments, """for ( el <- for ( d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""")
        ===
        SetType(
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
        SetType(
          RecordType(List(
            AttrType("E",
              RecordType(List(
                AttrType("children", ListType(
                  RecordType(List(
                    AttrType("age", IntType()))))),
                AttrType("manager", RecordType(List(
                  AttrType("name", StringType()),
                  AttrType("children", ListType(
                    RecordType(List(
                      AttrType("age", IntType()))))))))))),
            AttrType("M", IntType())))))
  }

  test("simple_inference") {
    assert(
      process(new World(Map("unknown" -> Source(SetType(TypeVariable()), EmptyLocation))), """for (l <- unknown) yield set (l + 2)""")
      ===
      SetType(IntType())
    )
  }

  test("inference_with_function") {
    assert(
      process(new World(Map("unknown" -> Source(SetType(TypeVariable()), EmptyLocation))), """for (l <- unknown, f := (\v -> v + 2)) yield set f(l)""")
      ===
      SetType(IntType())
    )

    //process(new World(Map("unknown" -> Source(SetType(TypeVariable()), EmptyLocation))), """for (a <- unknown, b <- unknown2, f := (\v -> v + b)) yield set (f(a) + 1)""")
  }

  test("infer_across_bind") {    assert(
    process(new World(Map("unknown" -> Source(SetType(TypeVariable()), EmptyLocation))), """for (j <- unknown, v := j) yield set (v + 0)""")
    ===
    SetType(IntType())
    )
    // what we really want to assert is that j is of type int and unknown is of type Set(int)
  }



}