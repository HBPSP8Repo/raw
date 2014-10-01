package raw.repl

import raw._
import raw.repl._
import raw.catalog._

class ReplTest extends org.scalatest.FunSuite {

  /** Test cases.
   *  
   *  Comparisons based on string outputs sound hacky, but the alternative - generating a algebra tree in code - is unreadable.
   *  TODO: Create a parser for the algebra to use to create algebra plans directly.
   *  
   */
  
  /** Simple CERN-like query.
   */
  test("cern_events") {
    val events =
      ListType(
        RecordType(List(
          Attribute("RunNumber", IntType),
          Attribute("lbn", IntType),
          Attribute("muon", RecordType(List(
            Attribute("mu_pt", FloatType),
            Attribute("mu_eta", FloatType)))))))
    
    val catalog = new Catalog(Map("Events" -> events))
    
    val alg = Repl.run(
      "for (e <- Events, e.RunNumber > 100) yield set (muon := e.muon)",
      catalog)
      
    val output = algebra.AlgebraPrettyPrinter(alg)
    val expected = """
Reduce set [ e = ( muon := <arg0>.muon ) ] [ p = true ]
  | Select [ p = ( ( <arg0>.RunNumber > 100 ) and true ) ] 
  |   | Scan Events
"""    
    assert(output == expected.trim())
  }
  
  /** Query taken from [1], page 12.
   */
  test("paper_query_1") {
    val courses = 
      SetType(
        RecordType(List(
          Attribute("name", StringType))))
    val instructors =
      SetType(
        RecordType(List(
          Attribute("name", StringType),
          Attribute("address", StringType),
          Attribute("teaches", courses))))
    val departments =
      SetType(
        RecordType(List(
          Attribute("name", StringType),
          Attribute("instructors", instructors))))
    
    val catalog = new Catalog(Map("Departments" -> departments))
    
    val alg = Repl.run(
      """for ( el <- for ( d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)""",
      catalog)

    val output = algebra.AlgebraPrettyPrinter(alg)
    val expected = """
Reduce set [ e = ( name := <arg1>.name, address := <arg1>.address ) ] [ p = true ]
  | Unnest [ path = <arg1>.teaches ] [ p = ( ( <arg2>.name = "cse5331" ) and true ) ] 
  |   | Unnest [ path = <arg0>.instructors ] [ p = ( true and true ) ] 
  |   |   | Select [ p = ( ( <arg0>.name = "CSE" ) and true ) ] 
  |   |   |   | Scan Departments
"""    
    assert(output == expected.trim())
  }
  
  /** Query taken from [1], page 32: "Query C".
   */
  test("paper_query_2") {
    val children =
      ListType(
        RecordType(List(
          Attribute("age", IntType))))
    val manager =
      RecordType(List(
        Attribute("name", StringType),
        Attribute("children", children)))
    val employees =
      SetType(
        RecordType(List(
          Attribute("children", children),
          Attribute("manager", manager))))
    
    val catalog = new Catalog(Map("Employees" -> employees))
    
    val alg = Repl.run(
      """for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)""",
      catalog)

    val output = algebra.AlgebraPrettyPrinter(alg)
    val expected = """
Reduce set [ e = ( E := <arg0>, M := <arg1> ) ] [ p = true ]
  | Nest sum [ e = 1 ] [ f = <arg0> ] [ p = <arg2> ] [ g = <arg1>,<arg2> ]
  |   | Nest and [ e = ( <arg1>.age > <arg2>.age ) ] [ f = <arg0>,<arg1> ] [ p = true ] [ g = <arg2> ]
  |   |   | OuterUnnest [ path = <arg0>.manager.children ] [ p = ( true and true ) ] 
  |   |   |   | OuterUnnest [ path = <arg0>.children ] [ p = ( true and true ) ] 
  |   |   |   |   | Select [ p = ( true and true ) ] 
  |   |   |   |   |   | Scan Employees
"""    
    assert(output == expected.trim())
  }
}