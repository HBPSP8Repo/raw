package raw
package calculus

trait RegexAnalyzer extends Analyzer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus.RegexConst
  import raw.regex.Regex.RegexNode
  import raw.regex.RegexSyntaxAnalyzer

  private lazy val regexAst: RegexConst => Either[RegexSyntaxAnalyzer.NoSuccess, RegexNode] = attr {
    r => RegexSyntaxAnalyzer(r.value)
  }

  private lazy val regexTree: RegexConst => raw.regex.Regex.Regex = attr {
    r => new raw.regex.Regex.Regex(regexAst(r).right.get)
  }

  private lazy val regex: RegexConst => raw.regex.RegexAnalyzer = attr {
    r => new raw.regex.RegexAnalyzer(regexTree(r))
  }

  private lazy val collectRegexErrors =
    collect[List, Seq[RawError]] {
      case r: RegexConst if regexAst(r).isLeft =>
        Seq(InvalidRegexSyntax(regexAst(r).left.get.toString))
      case r: RegexConst if regex(r).regexErrors.nonEmpty =>
        regex(r).regexErrors
    }

  lazy val regexErrors =
    collectRegexErrors(tree.root).flatten

  lazy val regexType: RegexConst => Type = attr {
    r =>
      val groups = regex(r).regexGroups
      assert(groups.nonEmpty)
      if (groups.length == 1)
        StringType()
      else {
        RecordType(Attributes(groups.zipWithIndex.map {
          case (raw.regex.Regex.RegexGroup(_, Some(idn)), _) => AttrType(idn, StringType())
          case (raw.regex.Regex.RegexGroup(_, None), idx) => AttrType(s"_${idx + 1}", StringType())
        }))
      }
  }

}
