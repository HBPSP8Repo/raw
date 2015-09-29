package raw

import calculus._
import com.typesafe.scalalogging.LazyLogging

sealed abstract class QueryError {
  def err: String
}

case class ParserError(desc: String) extends QueryError {
  def err = s"Invalid Query Input: $desc"
}

case class SemanticErrors(messages: Seq[Error]) extends QueryError {
  def err = s"Invalid Query: $messages"
}

case class InternalError(desc: String) extends QueryError {
  def err = s"Internal Error: $desc"
}

object Query extends LazyLogging {

  private def parse(query: String): Either[QueryError, Calculus.Calculus] = {
    SyntaxAnalyzer(query) match {
      case Right(ast) =>
        logger.debug(
          s"""
             |Input query:
             | << BEGIN >>
             |$query
             | << END >>
             |Parsed query:
             | << BEGIN >>
             |${CalculusPrettyPrinter(ast)}
             | << END >>
        """.stripMargin)
        Right(new Calculus.Calculus(ast))
      case Left(error) => Left(ParserError(error))
    }
  }

  private def analyze(tree: Calculus.Calculus, world: World): Either[QueryError, Type] = {
    val analyzer = new calculus.SemanticAnalyzer(tree, world)
    if (analyzer.errors.isEmpty) {
      Right(analyzer.tipe(tree.root))
    } else
      Left(SemanticErrors(analyzer.errors))
  }

  private def process(tree: Calculus.Calculus, world: World): Calculus.Calculus = {
    val optimized = Phases(tree, world)
    logger.debug(
      s"""
         |Optimized query:
         | << BEGIN >>
         |${CalculusPrettyPrinter(optimized.root)}
         | << END >>
       """.stripMargin)
    optimized
  }

  def apply(query: String, world: World): Either[QueryError, Calculus.Calculus] = {
    parse(query) match {
      case Right(tree) => analyze(tree, world) match {
        case Right(rootType) =>
          // (Training wheels): This (expensive) check is for testing purposes only!
          val finalTree = process(tree, world)
          analyze(finalTree, world) match {
            case Right(finalRootType) =>
              Right(finalTree)
//              if (rootType == finalRootType) {
//                Right(finalTree)
//              } else {
//                logger.debug(
//                  s"""
//                     |ERROR: Root types changed during unnesting!
//                     |Initial root type:
//                     |${PrettyPrinter(rootType)}
//                     |Final root type:
//                     |${PrettyPrinter(finalRootType)}
//                   """.stripMargin)
//                Left(InternalError(s"Root type changed after processing"))
//              }
            case Left(error) =>
              logger.debug(
                s"""
                   |ERROR: Could not type final tree.
                   |$error
                 """.stripMargin)
              Left(InternalError(s"Final root typing failed"))
          }
        case Left(error) => Left(error)
      }
      case Left(error) => Left(error)
    }
  }

}
