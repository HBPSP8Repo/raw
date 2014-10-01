package raw.repl

import raw._
import raw.calculus._
import raw.calculus.parser.TypeCheckerError
import raw.calculus.parser.ParserError
import raw.calculus.parser.PrimitiveTypeRequired
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
import raw.calculus.parser.ParserTypePrettyPrinter
import raw.calculus.normalizer.Normalizer
import raw.calculus.canonical.Canonical
import raw.catalog._
import raw.algebra.Algebra
import raw.algebra.unnester.Unnester
import raw.algebra.emitter.JsonEmitter

object Repl extends App {

  /** Given a query and a catalog:
   *  1) parse the query into the calculus form,
   *  2) normalize the calculus,
   *  3) put the calculus in canonical form,
   *  4) convert canonical form into algebra and,
   *  5) return the algebra.
   */
  def run(query: String, catalog: Catalog): Algebra = {
    //val st = System.nanoTime()
    val p = new parser.Parser(catalog)
    //println("catalog: " + (System.nanoTime() - st))
    
    //val st1 = System.nanoTime()
    val phase1 = p.parse(query)
    //println("parser: " + (System.nanoTime() - st1))
    
    //val st2 = System.nanoTime()
    val phase2 = Normalizer(phase1)
    //println("normalizer: " + (System.nanoTime() - st2))
    
    //val st3 = System.nanoTime()
    val phase3 = Canonical(phase2)
    //println("canonical: " + (System.nanoTime() - st3))
    
    //val st4 = System.nanoTime()
    val algebra = Unnester(phase3)
    //println("unnester: " + (System.nanoTime() - st4))
    algebra
  }

  private class ErrorFormatter(val err: String, input: String) {
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

  private def pprintTypeCheckerError(input: String, err: TypeCheckerError) = {
    val fmt = new ErrorFormatter(err.err, input)
    val errs = err match {
      case PrimitiveTypeRequired(e)            => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case BoolRequired(e)                     => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case NumberRequired(e)                   => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case MonoidMergeMismatch(m, e1, e2)      => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e1.pos.line, e1.pos.column, ParserTypePrettyPrinter(e1.parserType)); fmt.add(e2.pos.line, e2.pos.column, ParserTypePrettyPrinter(e2.parserType)) }
      case UnknownAttribute(name, pos)         => fmt.add(pos.line, pos.column, name)
      case RecordRequired(e)                   => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case IfResultMismatch(e1, e2)            => { fmt.add(e1.pos.line, e1.pos.column, ParserTypePrettyPrinter(e1.parserType)); fmt.add(e2.pos.line, e2.pos.column, ParserTypePrettyPrinter(e2.parserType)) }
      case FunctionApplicationMismatch(e1, e2) => { fmt.add(e1.pos.line, e1.pos.column, ParserTypePrettyPrinter(e1.parserType)); fmt.add(e2.pos.line, e2.pos.column, ParserTypePrettyPrinter(e2.parserType)) }
      case FunctionTypeRequired(e)             => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case CommutativeMonoidRequired(m, e)     => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType)) }
      case IdempotentMonoidRequired(m, e)      => { fmt.add(m.pos.line, m.pos.column, MonoidPrettyPrinter(m)); fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType)) }
      case CollectionTypeRequired(e)           => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
      case PredicateRequired(e)                => fmt.add(e.pos.line, e.pos.column, ParserTypePrettyPrinter(e.parserType))
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
  val manager = RecordType(List(Attribute("name", StringType), Attribute("children", children)))
  val employees = SetType(RecordType(List(Attribute("children", children), Attribute("manager", manager))))
  val employees1 = SetType(RecordType(List(Attribute("children", children), Attribute("manager", manager))))

  val cat = new Catalog(Map("Events" -> events, "Departments" -> departments, "Employees" -> employees, "Employees1" -> employees1, "Test" -> manager))

  var input: String = ""
  do {
    print("raw > ")
    val input = Console.readLine()
    try {
      val alg = run(input, cat)
      println(algebra.AlgebraPrettyPrinter(alg))
      println()
      
            println()
            println("JSON:")
            println(JsonEmitter(alg, cat))

    } catch {
      case e: TypeCheckerError => pprintTypeCheckerError(input, e)
      case e: ParserError      => pprintParserError(input, e)
      case e: RawException     => println("Error occurred: " + e)
    }
  } while (input != null)
}
