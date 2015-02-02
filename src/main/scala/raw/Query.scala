package raw

import org.kiama.util.Message
import algebra.{Unnester, LogicalAlgebra}
import calculus._
import executor.Executor
import executor.reference.ReferenceExecutor
import optimizer.Optimizer
import optimizer.reference.ReferenceOptimizer

abstract class QueryError
case class ParserError(error: String) extends QueryError
case class SemanticErrors(errors: Seq[Message]) extends QueryError

/** Interface for query results.
  *
  */
abstract class QueryResult {
  def value: Any

  // TODO: def toRDD
}

object Query {

  def parse(query: String, world: World): Either[QueryError, Calculus.Calculus] = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(query) match {
      case Right(ast) => Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  def analyze(t: Calculus.Calculus, w: World): Either[QueryError, SemanticAnalyzer] = {
    val analyzer = new SemanticAnalyzer(t, w)
    if (analyzer.errors.length == 0)
      Right(analyzer)
    else
      Left(SemanticErrors(analyzer.errors))
  }

  def unnest(tree: Calculus.Calculus, analyzer: SemanticAnalyzer, world: World): LogicalAlgebra.AlgebraNode = {
    val newTree = Simplifier(tree, world)
    Unnester(newTree)
  }

  def apply(query: String, world: World, optimizer: Optimizer = ReferenceOptimizer, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
    parse(query, world) match {
      case Right(tree) => analyze(tree, world) match {
        case Right(analyzer) => executor.execute(optimizer.optimize(unnest(tree, analyzer, world), world), world)
        case Left(err)       => Left(err)
      }
      case Left(err)       => Left(err)
    }
  }

}
