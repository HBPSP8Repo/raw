package raw.repl

import com.typesafe.scalalogging.StrictLogging
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.{LogicalAlgebra, LogicalAlgebraPrettyPrinter, Unnester}
import raw.calculus.Calculus.Exp
import raw.calculus.{Calculus, CalculusPrettyPrinter, SemanticAnalyzer}
import raw.util.Worlds

class ParseException(msg: String) extends Exception(msg)

class SemanticAnalysisException(msg: String) extends Exception(msg)

class RawContext extends StrictLogging {
  def parse(query: String): Exp = RawContext.parse(query)

  def analyzeSemantics(ast: Exp): Unit = RawContext.analyzeSemantics(ast)

  def analyzeSemantics(query: String): Unit = RawContext.analyzeSemantics(query)

  def unnest(ast: Exp): LogicalAlgebra.LogicalAlgebraNode = RawContext.unnest(ast)

  def unnest(query: String): LogicalAlgebra.LogicalAlgebraNode = RawContext.unnest(query)

  def prettyPrint(ast: Exp): String = RawContext.prettyPrint(ast)

  def prettyPrint(algebra: LogicalAlgebraNode): String = RawContext.prettyPrint(algebra)
}

object RawContext extends StrictLogging {
  def parse(query: String): Exp = {
    raw.calculus.SyntaxAnalyzer(query) match {
      case Right(ast) => ast
      case Left(error) => throw new ParseException(error)
    }
  }

  def prettyPrint(ast: Exp): String = {
    CalculusPrettyPrinter(ast, 100)
  }

  def prettyPrint(algebra: LogicalAlgebraNode): String = {
    LogicalAlgebraPrettyPrinter(algebra)
  }

  def analyzeSemantics(query: String): Unit = {
    analyzeSemantics(parse(query))
  }

  def analyzeSemantics(ast: Exp): Unit = {
    val t = new Calculus.Calculus(ast)
    val analyzer = new SemanticAnalyzer(t, Worlds.world)
    if (analyzer.errors.nonEmpty) {
      val str = analyzer.errors
        .zipWithIndex
        .map({ case (msg, i) => s"[${msg.pos}] ${msg.label}\n${msg.pos.longString}" })
        .mkString("\n")
      throw new SemanticAnalysisException(s"${analyzer.errors.length} errors found\n${str}")
    }
  }

  def unnest(ast: Exp): LogicalAlgebraNode = {
    val tree = new Calculus.Calculus(ast)
    Unnester(tree, Worlds.world)
  }

  def unnest(query: String): LogicalAlgebraNode = {
    val exp = parse(query)
    analyzeSemantics(exp)
    unnest(exp)
  }
}
