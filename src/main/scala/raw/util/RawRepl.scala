package raw.util

import org.kiama.util.{Console, REPL, REPLConfig}
import raw._
import raw.algebra.{LogicalAlgebra, Unnester}
import raw.calculus.{CalculusPrettyPrinter, Calculus, SemanticAnalyzer, SyntaxAnalyzer}
import raw.executor.Executor
import raw.executor.reference.ReferenceExecutor

object RawRepl extends REPL {
//  val world = World()
//
//  def apply(query: String, world: World, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
//    parse(query, world) match {
//      case Right(tree) => analyze(tree, world) match {
//        case None => executor.execute(unnest(tree, world), world)
//        case Some(err) => Left(err)
//      }
//      case Left(err) => Left(err)
//    }
//  }

//  def analyze(tree: Calculus.Calculus, w: World): Option[QueryError] = {
//    val analyzer = new SemanticAnalyzer(tree, world)
//    if (analyzer.errors.length == 0)
//      None
//    else
//      Some(SemanticErrors(analyzer.errors))
//  }
//
//  def unnest(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode = Unnester(tree, world)

  override def banner: String = "Raw REPL"

  override def processline(query: String, console: Console, config: REPLConfig): Option[REPLConfig] = {
    SyntaxAnalyzer(query) match {
      case Right(ast) => {
        val calculus = new Calculus.Calculus(ast)
        println(s"AST: $ast")
        val pretty = CalculusPrettyPrinter(ast, 200)
        println(s"Expression: $pretty")
      }
      case Left(error) => println(s"Parsing error: $error")
    }
    Some(config)
  }
}
