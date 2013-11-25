package raw.repl

import raw._
import raw.calculus._
import raw.calculus.normalizer._
import raw.calculus.canonical._
import raw.catalog._

/** FIXME:
 *  [MSB] Clean-up pretty printers: they're messy. Use scala.text._ instead.
 *  [MSB] pprint will render badly when errors cross multiple lines. See use of 'c'. 
 */

object Repl extends App {
  def pprintType(t: MonoidType): String =
    t match {
      case BoolType => "bool"
	  case FloatType => "float"
	  case IntType => "int"
	  case StringType => "string"
	  case RecordType(atts) => "record(" + atts.map(att => att.name + " = " + pprintType(att.monoidType)).mkString(", ") + ")"
	  case SetType(t) => "set(" + pprintType(t) + ")"
	  case BagType(t) => "bag(" + pprintType(t) + ")"
	  case ListType(t) => "list(" + pprintType(t) + ")"
	  case _ => "unknown"
	}
  
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

  def pprintBinaryOperator(op: BinaryOperator): String =
    op match {
      case op : Eq => "="
      case op : Neq => "<>"
      case op : Ge => ">="
      case op : Gt => ">"
      case op : Le => "<="
      case op : Lt => "<"
      case op : Add => "+"
      case op : Sub => "-"
      case op : Mult => "*"
      case op : Div => "/"
    }

  def pprintMonoid(m: Monoid): String =
    m match {
      case m : SumMonoid => "sum"
      case m : MultiplyMonoid => "multiply"
      case m : MaxMonoid => "max"
      case m : OrMonoid => "or"
      case m : AndMonoid => "and"
      case m : SetMonoid => "set"        
      case m : BagMonoid => "bag"
      case m : ListMonoid => "list"
    }
  
  def pprintTypeCheckerError(input: String, err: TypeCheckerError) = {
    val fmt = new ErrorFormatter(err.err, input)
    val errs = err match {
      case BinaryOperationMismatch(op, e1, e2) => { fmt.add(op.pos.line, op.pos.column, pprintBinaryOperator(op)); fmt.add(e1.pos.line, e1.pos.column, pprintType(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, pprintType(e2.monoidType)) }
      case BoolRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))   
      case NumberRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))
      case MonoidMergeMismatch(m, e1, e2) => { fmt.add(m.pos.line, m.pos.column, pprintMonoid(m)); fmt.add(e1.pos.line, e1.pos.column, pprintType(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, pprintType(e2.monoidType)) }
      case UnknownAttribute(name, pos) => fmt.add(pos.line, pos.column, name)
      case RecordRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))
      case IfResultMismatch(e1, e2) => { fmt.add(e1.pos.line, e1.pos.column, pprintType(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, pprintType(e2.monoidType)) }
      case FunctionApplicationMismatch(e1, e2) => { fmt.add(e1.pos.line, e1.pos.column, pprintType(e1.monoidType)); fmt.add(e2.pos.line, e2.pos.column, pprintType(e2.monoidType)) }
      case FunctionTypeRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))
      case CommutativeMonoidRequired(m, e) => { fmt.add(m.pos.line, m.pos.column, pprintMonoid(m)); fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType)) }
      case IdempotentMonoidRequired(m, e) => { fmt.add(m.pos.line, m.pos.column, pprintMonoid(m)); fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType)) }
      case CollectionTypeRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))
      case PredicateRequired(e) => fmt.add(e.pos.line, e.pos.column, pprintType(e.monoidType))
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
  
  def pprintExpr(e: Expression): String = e match {
    case Null() => "null"
    case BoolConst(v) => if (v) "true" else "false"
    case IntConst(v) => v.toString()
    case FloatConst(v) => v.toString()
    case StringConst(v) => "\"" + v.toString() + "\""
    case Variable(_) => "v" + e.hashCode().toString()
    case ClassExtent(_, id) => "`" + id + "`"
    case RecordProjection(_, e, name) => pprintExpr(e) + "." + name
    case RecordConstruction(_, atts) => "( " + atts.map(att => att.name + " := " + pprintExpr(att.e)).mkString(", ") + " )"
    case IfThenElse(_, e1, e2, e3) => "if " + pprintExpr(e1) + " then " + pprintExpr(e2) + " else " + pprintExpr(e3)
    case BinaryOperation(_, op, e1, e2) => "( " + pprintExpr(e1) + " " + pprintBinaryOperator(op) + " " + pprintExpr(e2) + " )"
    case FunctionAbstraction(_, v, e) => "\\" + pprintExpr(v) + " : " + pprintExpr(e) 
    case FunctionApplication(_, e1, e2) => pprintExpr(e1) + "(" + pprintExpr(e2) + ")"
    case EmptySet() => "{}"
    case EmptyBag() => "bag{}"
    case EmptyList() => "[]"
    case ConsCollectionMonoid(_, SetMonoid(), e) => "{ " + pprintExpr(e) + " }"
    case ConsCollectionMonoid(_, BagMonoid(), e) => "bag{ " + pprintExpr(e) + " }"
    case ConsCollectionMonoid(_, ListMonoid(), e) => "[ " + pprintExpr(e) + " ]"
    case MergeMonoid(_, m, e1, e2) => "( " + pprintExpr(e1) + " " + pprintMonoid(m) + " " + pprintExpr(e2) + " )"
    case Comprehension(_, m, e, qs) => "for ( " + qs.map(pprintExpr(_)).mkString(", ") + " ) yield " + pprintMonoid(m) + " " + pprintExpr(e)
    case Generator(v, e) => pprintExpr(v) + " <- " + pprintExpr(e)
    case Not(e) => "not(" + pprintExpr(e) + ")"
    case Bind(v, e) => pprintExpr(v) + " := " + pprintExpr(e)
  }
  
  /* Built-in class extends */
  val events = ListType(RecordType(List(Attribute("RunNumber", IntType), Attribute("lbn", IntType), Attribute("muon", RecordType(List(Attribute("mu_pt", FloatType), Attribute("mu_eta", FloatType)))))))
  
  val courses = SetType(RecordType(List(Attribute("name", StringType))))
  val instructors = SetType(RecordType(List(Attribute("name", StringType), Attribute("address", StringType), Attribute("teaches", courses))))
  val departments = SetType(RecordType(List(Attribute("name", StringType), Attribute("instructors", instructors))))
  // Sample query:
  // for ( el <- for ( d <- `Departments`, d.name = "CSE" ) yield set d.instructors, e <- el, for (c <- e.teaches) yield or c.name = "cse5331") yield set (name := e.name, address := e.address)
  
  val cat = new Catalog(Map("events" -> events, "Departments" -> departments))
  
  val p = new Parser(cat)
  
  var input: String = ""
  do {
    print("raw > ")
    val input = Console.readLine()
    try {
      val e = p.parse(input)
      println()
      println("Parsed Expression:")
      println(pprintExpr(e))
      println("Result Type: " + pprintType(e.monoidType))
      println()
      println("Normalized Expression:")
      val norme = Normalizer.normalize(e)
      println(pprintExpr(norme))
      println("Result Type: " + pprintType(norme.monoidType))
      println()
      println(if (e != norme) "Expression normalized" else "Expression already normalized")
      println()
      println("Canonical Form:")
      val cane = Canonical.canonical(e)
      println(pprintExpr(cane))
      println("Result Type: " + pprintType(cane.monoidType))
      println()
      println(if (norme != cane) "Expression put in canonical form" else "Expression already in canonical form")
      
    } catch {
      case e : TypeCheckerError => pprintTypeCheckerError(input, e)
      case e : ParserError => pprintParserError(input, e)
      case e : RawException => println("Error occurred: " + e)
    }
  } while (input != null)
}
