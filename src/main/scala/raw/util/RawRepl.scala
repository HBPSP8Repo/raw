package raw.util

import org.kiama.util.{Console, REPL, REPLConfig}
import raw._
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.{LogicalAlgebra, Unnester}
import raw.calculus.{CalculusPrettyPrinter, Calculus, SemanticAnalyzer, SyntaxAnalyzer}
import raw.executor.Executor
import raw.executor.reference.ReferenceExecutor

object RawRepl extends REPL {

  override def banner: String = "Raw REPL"

  val students = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("birthYear", IntType()), AttrType("office", StringType()), AttrType("department", StringType())))),
    "src/test/data/smokeTest/students.csv",
    TextCsv(","))

  val profs = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("office", StringType())))),
    "src/test/data/smokeTest/profs.csv",
    TextCsv(","))

  val departments = LocalFileLocation(
    ListType(RecordType(List(AttrType("name", StringType()), AttrType("discipline", StringType()), AttrType("prof", StringType())))),
    "src/test/data/smokeTest/departments.csv",
    TextCsv(","))

  val world: World = new World(Map(
    "students" -> students,
    "profs" -> profs,
    "departments" -> departments))

  def printWorld(): Unit = {
    val str = world.catalog
      .map({ case (name: String, dl: DataLocation) => name + " " + dl})
      .mkString("/n")
    println(str)
  }

  def syntaxAnalysis(query: String): Calculus.Calculus = {
    println("Phase 1: Parsing")
    SyntaxAnalyzer(query) match {
      case Right(ast) => {
        val calculus = new Calculus.Calculus(ast)
        println(s"  AST: $ast")
        val pretty = CalculusPrettyPrinter(ast, 200)
        println(s"  Expression: $pretty")
        calculus
      }
      case Left(error) => {
        throw new Exception(s"Parsing error: $error")
      }
    }
  }

  def semanticAnalysis(tree: Calculus.Calculus) {
    println("Phase 2: Semantic Analysis")
    val analyzer = new SemanticAnalyzer(tree, world)
    if (!analyzer.errors.isEmpty) {
      throw new Exception(analyzer.errors.mkString(", "))
    }
  }

  def unnest(tree: Calculus.Calculus): LogicalAlgebraNode = {
    println("Phase 3: Unnesting")
    // TODO: Can this ever fail?
    val logicalAlgebra = Unnester(tree, world)
    return logicalAlgebra
  }

  override def processline(query: String, console: Console, config: REPLConfig): Option[REPLConfig] = {
    val trimmedQuery = query.trim
    if (trimmedQuery.startsWith("""\""")) {
      executeCommand(trimmedQuery.tail, console, config)
    } else { 
      executeExpression(trimmedQuery, console, config)
    }
  }
  def executeCommand(command: String, console: Console, config: REPLConfig): Option[REPLConfig] = {
    command match {
      case "quit" => None
      case "showWorld" => printWorld(); Some(config)
      case s => println(s"Unknown command: $s");Some(config)
    }
  }

  def executeExpression(query: String, console: Console, config: REPLConfig): Option[REPLConfig] = {
    try {
      val calculusTree = syntaxAnalysis(query)

      semanticAnalysis(calculusTree)

      val logicalAlgebra = unnest(calculusTree)

      println(s"Result: $logicalAlgebra")
    } catch {
      case e: Throwable => println("Error: " + e)
    }
    Some(config)
  }
}
