package raw.util

import org.kiama.util._
import raw._
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.Unnester
import raw.calculus.{Calculus, CalculusPrettyPrinter, SemanticAnalyzer, SyntaxAnalyzer}

import scala.collection.immutable.Seq

abstract class RawReplConfig(args: Array[String]) extends REPLConfig(args) {
  // configure how the help message is displayed
  version("RawRepl 0.0.1 (c) 2015 RAW Labs")

  banner( """Usage: rawRepl [options]...
            |REPL for raw query language
            |Options:
            | """.stripMargin)

  footer("\nFooter placeholder, if needed!")

  // Definition of options and command line arguments
  val world = opt[String](default = Some("world"), descr = "Path to directory containing catalog")
}


object RawRepl extends REPLBase[RawReplConfig] {
  override def banner: String = "Raw REPL"

  /* Override base class method to perform custom initialization of the REPL after the configuration is loaded but
   * before entering interactive mode.
   */
  override def createAndInitConfig(args: Array[String], output: Emitter, error: Emitter): RawReplConfig = {
    val config = super.createAndInitConfig(args, output, error)
    initializeRawRepl(config)
    config
  }

  override def createConfig(args: Array[String], out: Emitter, err: Emitter): RawReplConfig = {
    new RawReplConfig(args) {
      lazy val output = out
      lazy val error = err
    }
  }

  /*
   * Loads the catalog and performs other initialization tasks before entering interactive mode
   * Called after the configuration object from Scallop is loaded and initialized.
   */
  def initializeRawRepl(config: RawReplConfig) = {
    println("Initializing world: " + config.world())
    loadWorld(config.world())
  }

  def loadWorld(worldDirectory: String) = {
    println("TODO: Catalog loading not yet implemented")
  }

  override def processline(query: String, console: Console, config: RawReplConfig): Option[RawReplConfig] = {
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
      case "printConfig" => println(config.summary); Some(config)
      case "printWorld" => printWorld(); Some(config)
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

      executeQuery(logicalAlgebra)

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
        val pretty = CalculusPrettyPrinter(ast, 100)
        println(s"  Parsed expression: $pretty")
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

  def executeQuery(node: LogicalAlgebraNode) = {
    println("Phase 4: Execution")
    println("TODO: Query execution not yet implemented.")
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