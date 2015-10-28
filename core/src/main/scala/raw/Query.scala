package raw

import calculus._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.immutable.List

/** JSON-friendly version of the Error class.
  */
case class SemanticError(errorType: String, positions: List[RawParserPosition], message: String)

sealed abstract class QueryError {
  def err: String
}

case class ParserError(position: RawParserPosition, message: String) extends QueryError {
  def err = s"Invalid Query Input: $message"
}

case class SemanticErrors(errors: List[SemanticError]) extends QueryError {
  def err = s"Error(s) found:\n${errors.mkString("\n")}"
}

case class InternalError(desc: String) extends QueryError {
  def err = s"Internal Error: $desc"
}

object RawPositionExtractor {
  def apply(e: Error): List[RawParserPosition] = e match {
    case MultipleDecl(_, pos) => List(RawPositionExtractor(pos))
    case UnknownDecl(_, pos) => List(RawPositionExtractor(pos))
    case AmbiguousIdn(_, pos) => List(RawPositionExtractor(pos))
    case PatternMismatch(_, _, pos) => List(RawPositionExtractor(pos))
    case UnknownPartition(_, pos) => List(RawPositionExtractor(pos))
    case UnknownStar(_, pos) => List(RawPositionExtractor(pos))
    case IncompatibleMonoids(_, _, pos) => List(RawPositionExtractor(pos))
    case IncompatibleTypes(_, _, pos1, pos2) => List(RawPositionExtractor(pos1), RawPositionExtractor(pos2))
    case UnexpectedType(_, _, _, pos) => List(RawPositionExtractor(pos))
    case IllegalStar(_, pos) => List(RawPositionExtractor(pos))
  }

  def apply(pos: Option[RawParserPosition]): RawParserPosition = pos match {
    case Some(p: RawParserPosition) => RawParserPosition(RawPosition(p.begin.line, p.begin.column), RawPosition(p.end.line, p.end.column))
  }
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
      case Left(error: SyntaxAnalyzer.NoSuccess) =>
        // TODO: Sync w/ Cesar but could change ParserError signature to take RawPosition instead of RawParserPosition since there's no notion of end anyway
        val pos = RawPosition(error.next.pos.line, error.next.pos.column)
        Left(ParserError(RawParserPosition(pos, pos), error.toString))
    }
  }

  private def analyze(tree: Calculus.Calculus, world: World, queryString: String): Either[QueryError, Type] = {
    val analyzer = new calculus.SemanticAnalyzer(tree, world, queryString)
    if (analyzer.errors.isEmpty) {
      Right(analyzer.tipe(tree.root))
    } else {
      val semanticErrors = analyzer.errors.map((e: Error) => SemanticError(e.getClass.getSimpleName, RawPositionExtractor(e), ErrorsPrettyPrinter(e)))
      Left(SemanticErrors(semanticErrors.to))
    }
  }

  private def process(tree: Calculus.Calculus, world: World, queryString: String): Calculus.Calculus = {
    val optimized = Phases(tree, world, queryString)
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
      case Right(tree) => analyze(tree, world, query) match {
        case Right(rootType) =>
          // (Training wheels): This (expensive) check is for testing purposes only!
          val finalTree = process(tree, world, query)
          analyze(finalTree, world, query) match {
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
