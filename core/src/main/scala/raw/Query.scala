package raw

import org.kiama.util.Message
import calculus._

sealed abstract class QueryError {
  def err: String
}
case class ParserError(desc: String) extends QueryError {
  def err = s"Parser error: $desc"
}

case class SemanticErrors(messages: Seq[Message]) extends QueryError {
  def err = s"Semantic error: $messages"
}

object Query {

  def parse(query: String): Either[QueryError, Calculus.Calculus] = {
    SyntaxAnalyzer(query) match {
      case Right(ast) => Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  def analyze(tree: Calculus.Calculus, world: World): Option[QueryError] = {
    None
//    val analyzer = new SemanticAnalyzer(tree, world)
//    if (analyzer.errors.length == 0)
//      None
//    else
//      Some(SemanticErrors(analyzer.errors))
  }

  def apply(query: String, world: World): Either[QueryError, Calculus.Exp] = {
    parse(query) match {
      case Right(tree) => analyze(tree, world) match {
        case None        => Right(calculus.Unnester(tree, world).root)
        case Some(error) => Left(error)
      }
      case Left(error) => Left(error)
    }
  }

}
