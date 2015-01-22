package raw

import org.kiama.util.Message
import algebra.{ LogicalAlgebra => LogicalAlgebra }
import calculus.SyntaxAnalyzer
import calculus.Unnester
import calculus.Calculus
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

  def parse(q: String, w: World): Either[QueryError, Calculus.Comp] = {
    val parser = new SyntaxAnalyzer()
    parser.makeAST(q) match {
      case Right(ast) => Right(ast)
      case Left(error) => Left(ParserError(error))
    }
  }

  def unnest(q: String, w: World, ast: Calculus.Comp): Either[QueryError, LogicalAlgebra.AlgebraNode] = {
    val unnester = new Unnester { val world = w }
    val errors = unnester.errors(ast)
    if (errors.length > 0) {
      Left(SemanticErrors(errors))
    } else {
      Right(unnester.unnest(ast))
    }
  }

  def apply(q: String, w: World, optimizer: Optimizer = ReferenceOptimizer, executor: Executor = ReferenceExecutor): Either[QueryError, QueryResult] = {
    parse(q, w) match {
      case Right(comp) => unnest(q, w, comp) match {
        case Right(algebra) => executor.execute(optimizer.optimize(algebra, w), w)
        case Left(err)      => Left(err)
      }
      case Left(err) => Left(err)
    }
  }

}
