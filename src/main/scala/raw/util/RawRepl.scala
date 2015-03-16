package raw.util

import org.kiama.util._
import raw._
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.Unnester
import raw.calculus.{Calculus, CalculusPrettyPrinter, SemanticAnalyzer, SyntaxAnalyzer}

import scala.collection.immutable.Seq

abstract class RawReplConfig(args: Array[String]) extends REPLConfig(args) {
  version("RawRepl 0.0.1 (c) 2015 RAW Labs")

  banner( """Usage: rawRepl [options]...
            |REPL for raw query language
            |Options:
            | """.stripMargin)

  footer("\nFooter placeholder, if needed!")

  val world = opt[String](default = Some("world"), descr = "Path to directory containing catalog")
}

object RawRepl extends REPLBase[RawReplConfig] {
  override def banner: String = "Raw REPL"

  override def createConfig(args: Array[String],
                            out: Emitter = new OutputEmitter,
                            err: Emitter = new ErrorEmitter): RawReplConfig = {
    new RawReplConfig(args) {
      lazy val output = out
      lazy val error = err
    }
  }

  // TODO: load world from disk

  override def processline(query: String, console: Console, config: RawReplConfig): Option[RawReplConfig] = {
    println("Configuration summary: " + config.summary)

    val trimmedQuery = query.trim
    if (trimmedQuery.startsWith( """\""")) {
      executeCommand(trimmedQuery.tail, console, config)
    } else {
      executeExpression(trimmedQuery, console, config)
    }
  }

  def executeCommand(command: String, console: Console, config: RawReplConfig): Option[RawReplConfig] = {
    command match {
      case "quit" => None
      case "help" => config.printHelp(); Some(config)
      case "showWorld" => printWorld(); Some(config)
      case s => println(s"Unknown command: $s"); Some(config)
    }
  }

  def printWorld(): Unit = {
    val str = Worlds.world.catalog
      .map({ case (name: String, dl: DataLocation) => name + " " + dl})
      .mkString("/n")
    println(str)
  }

  def executeExpression(query: String, console: Console, config: RawReplConfig): Option[RawReplConfig] = {
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

  def syntaxAnalysis(query: String): Calculus.Calculus = {
    println("Phase 1: Parsing")
    SyntaxAnalyzer(query) match {
      case Right(ast) =>
        val calculus = new Calculus.Calculus(ast)
        println(s"  AST: $ast")
        val pretty = CalculusPrettyPrinter(ast, 200)
        println(s"  Expression: $pretty")
        calculus
      case Left(error) => throw new Exception(s"Parsing error: $error")
    }
  }

  def semanticAnalysis(tree: Calculus.Calculus) {
    println("Phase 2: Semantic Analysis")
    val analyzer = new SemanticAnalyzer(tree, Worlds.world)
    if (analyzer.errors.nonEmpty) {
      throw new Exception(analyzer.errors.mkString(", "))
    }
  }

  def unnest(tree: Calculus.Calculus): LogicalAlgebraNode = {
    println("Phase 3: Unnesting")
    // TODO: Can this ever fail?
    val logicalAlgebra = Unnester(tree, Worlds.world)
    logicalAlgebra
  }


}


object Worlds {
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
}