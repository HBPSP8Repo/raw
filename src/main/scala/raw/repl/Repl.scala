package raw.repl

import raw._
import raw.calculus._
import raw.calculus.parser.TypeCheckerError
import raw.calculus.parser.ParserError
import raw.calculus.parser.BinaryOperationMismatch
import raw.calculus.parser.BoolRequired
import raw.calculus.parser.NumberRequired
import raw.calculus.parser.MonoidMergeMismatch
import raw.calculus.parser.UnknownAttribute
import raw.calculus.parser.RecordRequired
import raw.calculus.parser.IfResultMismatch
import raw.calculus.parser.FunctionApplicationMismatch
import raw.calculus.parser.FunctionTypeRequired
import raw.calculus.parser.CommutativeMonoidRequired
import raw.calculus.parser.IdempotentMonoidRequired
import raw.calculus.parser.CollectionTypeRequired
import raw.calculus.parser.PredicateRequired
import raw.calculus.parser.RootScope
import raw.calculus.normalizer.Normalizer
import raw.calculus.canonical.Canonical
import raw.catalog._
import raw.algebra.unnester.Unnester

/** FIXME:
 *  [MSB] Clean-up pretty printers: they're messy. Use scala.text._ instead.
 *  [MSB] pprint will render badly when errors cross multiple lines. See use of 'c'. 
 */

object Repl extends App {
  
  class ErrorFormatter(val err: String, input: String) {
    var map = Map[Int, collection.mutable.Map[Int, String]]()
    
    val inlines = input.split("\n")
    
    def add(line: Int, col: Int, s: String) = {
      val line0 = line - 1
      val col0 = col - 1
      if (!map.isDefinedAt(line0)) {
        map += (line0 -> collection.mutable.Map(col0 -> s))
      } else {
        map(line0) += (col0 -> s)
      }
    }
    
    def pprint() = {
      println("Error: " + err)
      var c = 1
      val lines = map.keys.toList.sortWith(_ < _)
      for (line <- lines) {
        val cols = map(line).keys.toList.sortWith(_ < _)

        println()
        
        val header = "Line " + line + ": "
        print(header)
        println(inlines(line))
        
        for (i <- 0 until header.size) print(" ")
        for (i <- 0 to cols.last)
          if (cols.contains(i)) print("^") else print(" ")
        println()
        
        for (i <- 0 until header.size) print(" ")
        for (i <- 0 to cols.last) 
          if (cols.contains(i)) { print(c); c += 1 } else print(" ")
        println()
        
        println()
        for (i <- 0 until cols.size) {
          println("found " + map(line)(cols(i)) + " in position " + (i + 1))
        }
      }
    }
  }
  
  def pprintTypeCheckerError(input: String, err: TypeCheckerError) = {
    val fmt = new ErrorFormatter(err.err, input)
    val errs = err match {
      case BinaryOperationMismatch(op, e1, e2) => { fmt.add(op.pos.line, op.pos.column, BinaryOperatorPrettyPrinter(op)); fmt.add(e1.pos.line, e1.pos.column, MonoidTypePrettyPrinter(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, MonoidTypePrettyPrinter(e2.monoidType)) }
      case BoolRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))   
      case NumberRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))
      case MonoidMergeMismatch(m, e1, e2) => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e1.pos.line, e1.pos.column, MonoidTypePrettyPrinter(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, MonoidTypePrettyPrinter(e2.monoidType)) }
      case UnknownAttribute(name, pos) => fmt.add(pos.line, pos.column, name)
      case RecordRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))
      case IfResultMismatch(e1, e2) => { fmt.add(e1.pos.line, e1.pos.column, MonoidTypePrettyPrinter(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, MonoidTypePrettyPrinter(e2.monoidType)) }
      case FunctionApplicationMismatch(e1, e2) => { fmt.add(e1.pos.line, e1.pos.column, MonoidTypePrettyPrinter(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, MonoidTypePrettyPrinter(e2.monoidType)) }
      case FunctionTypeRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))
      case CommutativeMonoidRequired(m, e) => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType)) }
      case IdempotentMonoidRequired(m, e) => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType)) }
      case CollectionTypeRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))
      case PredicateRequired(e) => fmt.add(e.pos.line, e.pos.column, MonoidTypePrettyPrinter(e.monoidType))
    }
    fmt.pprint()
  }
  
  def pprintParserError(input: String, e: ParserError) = {
    
    def pprintInputLine(input: String, i: Int) =
      println(input.split("\n")(i - 1))
    
    def pprintAtCol(i: Int, s: String) = {
      for (n <- 1 until i)
        print(" ")
      println(s)
    }
      
    pprintInputLine(input, e.line); pprintAtCol(e.column, "^"); pprintAtCol(e.column, e.err) 
  }
  
  /* Built-in class extends */
  val events = ListType(RecordType(List(Attribute("RunNumber", IntType), Attribute("lbn", IntType), Attribute("muon", RecordType(List(Attribute("mu_pt", FloatType), Attribute("mu_eta", FloatType)))))))
  
  val courses = SetType(RecordType(List(Attribute("name", StringType))))
  val instructors = SetType(RecordType(List(Attribute("name", StringType), Attribute("address", StringType), Attribute("teaches", courses))))
  val departments = SetType(RecordType(List(Attribute("name", StringType), Attribute("instructors", instructors))))
  
  val children = ListType(RecordType(List(Attribute("age", IntType))))
  val manager = SetType(RecordType(List(Attribute("name", StringType), Attribute("children", children))))
  val employees = SetType(RecordType(List(Attribute("children", children), Attribute("manager", manager))))

  val cat = new Catalog(Map("events" -> events, "Departments" -> departments, "Employees" -> employees))
  
  val rootScope = new RootScope()
  for (name <- cat.classNames)
    rootScope.bind(name, parser.Variable(cat.getClassType(name)))
  
  val p = new parser.Parser(rootScope)
  println(rootScope.bindings)
  
  var input: String = ""
  do {
    print("raw > ")
    val input = Console.readLine()
    try {
      val e = p.parse(input)
      
      println()
      println("Parsed Expression:")
      println(parser.CalculusPrettyPrinter(e))
      println("Result Type: " + MonoidTypePrettyPrinter(e.monoidType))
      
      println()
      println("Normalized Expression:")
      val norme = Normalizer.normalize(e)
      println(normalizer.CalculusPrettyPrinter(norme))
      println("Result Type: " + MonoidTypePrettyPrinter(norme.monoidType))
      
      println()
      println("Canonical Form:")
      val cane = Canonical.canonical(norme)
      println(canonical.CalculusPrettyPrinter(cane))
      println("Result Type: " + MonoidTypePrettyPrinter(cane.monoidType))
      
      println()
      println("Algebra:")
      val alg = Unnester.unnest(cane)
      println(algebra.AlgebraPrettyPrinter(alg))
      println()
      
    } catch {
      case e : TypeCheckerError => pprintTypeCheckerError(input, e)
      case e : ParserError => pprintParserError(input, e)
      case e : RawException => println("Error occurred: " + e)
    }
  } while (input != null)
}

/* Sample queries:

for ( el <- for ( d <- Departments, d.name = "CSE") yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)  

for ( el <- for ( d <- Departments, y := d.name, if (not (y = "CSE")) then true else false ) yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)

The following query is UNSUPPORTED due to the use of path 'e.manager.children':
for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d <- e.manager.children) yield and c.age > d.age) yield sum 1)

for (e <- Employees) yield set (E := e, M := for (c <- e.children, for (d1 <- e.manager, d <- d1.children) yield and c.age > d.age) yield sum 1)
*/
