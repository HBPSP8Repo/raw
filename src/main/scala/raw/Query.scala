package raw

import org.kiama.util.Message
import algebra.{Unnester, LogicalAlgebra}
import calculus._
import executor.Executor
import executor.reference.ReferenceExecutor

abstract class QueryError
case class ParserError(error: String) extends QueryError
case class SemanticErrors(errors: Seq[Message]) extends QueryError

/** Interface for query results.
  *
  */
abstract class QueryResult {
  def value: Any
}

object Query {

  def parse(query: String): Either[QueryError, Calculus.Calculus] = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(query) match {
      case Right(ast) => Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  def analyze(tree: Calculus.Calculus, world: World): Option[QueryError] = {
    val analyzer = new SemanticAnalyzer(tree, world)
    if (analyzer.errors.length == 0)
      None
    else
      Some(SemanticErrors(analyzer.errors))
  }

  def unnest(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode =
    Unnester(tree, world)

  def apply(query: String, world: World, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
    parse(query) match {
      case Right(tree) => analyze(tree, world) match {
        case None        => executor.execute(unnest(tree, world), world)
        case Some(error) => Left(error)
      }
      case Left(error) => Left(error)
    }
  }

}
