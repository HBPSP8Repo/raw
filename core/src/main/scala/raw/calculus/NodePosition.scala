package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

/** Given a tree and an expression, return its begin and end position.
  */
trait NodePosition extends Attribution with Analyzer with LazyLogging {

  import Calculus._

  def beginPosition(n: CalculusNode): RawPosition = RawPosition(n.pos.line, n.pos.column)

  private def stripFromBegin(p: RawPosition): String = {
    assert(p.line > 0 && p.column > 0)
    queryString.split("\\n").drop(p.line - 1).mkString("\n").drop(p.column - 1)
  }

  private def reverseParser(n: CalculusNode) = n match {
    case _: Null => SyntaxAnalyzer.nullConst
    case _: BoolConst => SyntaxAnalyzer.boolConst
    case _: IntConst => SyntaxAnalyzer.numberConst
    case _: FloatConst => SyntaxAnalyzer.numberConst
    case _: StringConst => SyntaxAnalyzer.stringConst
    case _: RegexConst => SyntaxAnalyzer.regexConst
    case _: IdnDef => SyntaxAnalyzer.idnDef
    case _: IdnUse => SyntaxAnalyzer.idnUse
    case _: IdnExp => SyntaxAnalyzer.idnExp
    case _: RecordProj => SyntaxAnalyzer.recordProjExp
    case _: RecordCons => SyntaxAnalyzer.recordCons
    case _: IfThenElse => SyntaxAnalyzer.ifThenElse
    case _: InExp => SyntaxAnalyzer.inExp
    case _: FunApp => SyntaxAnalyzer.funAppExp
    case _: MultiCons => SyntaxAnalyzer.multiCons
    case _: Comp => SyntaxAnalyzer.comp
    case _: FunAbs => SyntaxAnalyzer.funAbs
    case _: Gen => SyntaxAnalyzer.gen
    case _: Bind => SyntaxAnalyzer.bind
    case _: ExpBlock => SyntaxAnalyzer.expBlock
    case _: Sum => SyntaxAnalyzer.sumExp
    case _: Max => SyntaxAnalyzer.maxExp
    case _: Min => SyntaxAnalyzer.minExp
    case _: Avg => SyntaxAnalyzer.avgExp
    case _: Count => SyntaxAnalyzer.countExp
    case _: Exists => SyntaxAnalyzer.existsExp
    case _: PatternIdn => SyntaxAnalyzer.patternIdn
    case _: PatternProd => SyntaxAnalyzer.patternProd
    case _: Partition => SyntaxAnalyzer.partition
    case _: Star => SyntaxAnalyzer.starExp
    case _: Select => SyntaxAnalyzer.select
    case _: Into => SyntaxAnalyzer.intoExp
    case _: ParseAs => SyntaxAnalyzer.parseAsExp
  }

  def endPosition(n: CalculusNode): RawPosition = n match {
    case BinaryExp(_, _, e2) => endPosition(e2)
    case MergeMonoid(_, _, e2) => endPosition(e2)
    case UnaryExp(_, e) => endPosition(e)
    case _ =>
      val begin = beginPosition(n)
      val stripQueryString = stripFromBegin(begin)
      val parser = reverseParser(n)
      SyntaxAnalyzer.parseSubstring(parser, stripQueryString) match {
        case SyntaxAnalyzer.Success(_, next) => RawPosition(begin.line + next.pos.line - 1, begin.column + next.pos.column - 1)
      }
  }

  lazy val parserPosition: CalculusNode => RawParserPosition = attr {
    case e => RawParserPosition(beginPosition(e), endPosition(e))
  }
}
