package raw

import org.kiama.util.Message
import algebra.{Unnester, Algebra}
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

  def parse(query: String, world: World): Either[QueryError, Calculus.Calculus] = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(query) match {
      case Right(ast) => Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  def analyze(t: Calculus.Calculus, w: World): Option[QueryError] = {
    val analyzer = new SemanticAnalyzer(t, w)
    if (analyzer.errors.length == 0)
      None
    else
      Some(SemanticErrors(analyzer.errors))
  }

  def unnest(tree: Calculus.Calculus, world: World): Algebra.OperatorNode = Unnester(tree, world)

  def apply(query: String, world: World, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
    parse(query, world) match {
      case Right(tree) => analyze(tree, world) match {
        case None            => executor.execute(unnest(tree, world), world)
        case Some(err)       => Left(err)
      }
      case Left(err)       => Left(err)
    }
  }

}
