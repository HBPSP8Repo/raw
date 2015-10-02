package raw

import calculus._
import com.typesafe.scalalogging.LazyLogging
import raw.calculus.Calculus.{IdnNode, Exp}


import scala.reflect.internal.util.NoPosition
import scala.util.parsing.input.{OffsetPosition, Position}

/* Json-friendly version of the Position class. This allows us to control the format of the JSON response sent to the 
REST client client. */
case class RawPosition(line:Int, column:Int, source:String)

/* Json-friendly version of the Error class. */
case class SemanticError(errorType:String, position:RawPosition, message:String, prettyMessage:String)

sealed abstract class QueryError {
  def err: String
}

case class ParserError(position:RawPosition, message:String, prettyMessage:String) extends QueryError {
  def err = s"Invalid Query Input: ${prettyMessage}"
}

case class SemanticErrors(errors: Seq[SemanticError]) extends QueryError {
  def err = s"Error(s) found:\n${errors.mkString("\n")}"
}

case class InternalError(desc: String) extends QueryError {
  def err = s"Internal Error: $desc"
}


object RawPositionExtractor {
  def apply(e: Error): RawPosition = {
    e match {
      case MultipleDecl(i) => RawPositionExtractor(Some(i.pos))
      case UnknownDecl(i) => RawPositionExtractor(Some(i.pos))
      case UnknownPartition(p) => RawPositionExtractor(Some(p.pos))
      case CollectionRequired(t, p) => RawPositionExtractor(p)
      case IncompatibleMonoids(m, t, p) => RawPositionExtractor(p)
      // IncompatibleTypes has two positions, but we can only return one. The pretty printer message will contain the full description.
      case IncompatibleTypes(t1, t2, p1, p2) => RawPositionExtractor(p1)
      case UnexpectedType(t, _, Some(desc), p) => RawPositionExtractor(p)
      case UnexpectedType(t, expected, None, p) => RawPositionExtractor(p)
    }
  }

  def apply(pos:Option[Position]): RawPosition = {
    pos match {
      case Some(op:OffsetPosition) => RawPosition(op.line, op.column, op.source.toString)
      case Some(p:Position) => RawPosition(p.line, p.column, "")
      case None => RawPosition(0, 0, "")
    }
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
        Left(ParserError(RawPositionExtractor(Some(error.next.pos)), error.msg, error.toString))
    }
  }

  private def analyze(tree: Calculus.Calculus, world: World): Either[QueryError, Type] = {
    val analyzer = new calculus.SemanticAnalyzer(tree, world)
    if (analyzer.errors.isEmpty) {
      Right(analyzer.tipe(tree.root))
    } else {
      val semanticErrors = analyzer.errors.map((e: Error) => SemanticError(e.getClass.getSimpleName, RawPositionExtractor(e), e.toString, ErrorsPrettyPrinter(e)))
      val sortedSemanticErros = semanticErrors.sortBy(se => (se.position.line, se.position.column))
      Left(SemanticErrors(sortedSemanticErros))
    }
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
