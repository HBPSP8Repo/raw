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

  private lazy val regexTree: RegexConst => Option[raw.regex.Regex.Regex] = attr {
    r =>
      if (regexAst(r).isRight)
        Some(new raw.regex.Regex.Regex(regexAst(r).right.get))
      else
        None
  }

  private lazy val regex: RegexConst => Option[raw.regex.RegexAnalyzer] = attr {
    r =>
      if (regexTree(r).isDefined)
        Some(new raw.regex.RegexAnalyzer(regexTree(r).get))
      else
        None
  }

  private lazy val collectRegexErrors =
    collect[List, Seq[RawError]] {
      case r: RegexConst if regexAst(r).isLeft =>
        Seq(InvalidRegexSyntax(regexAst(r).left.get.toString))
      case r: RegexConst if regex(r).isDefined && regex(r).get.regexErrors.nonEmpty =>
        regex(r).get.regexErrors
    }

  lazy val regexErrors =
    collectRegexErrors(tree.root).flatten

  lazy val regexType: RegexConst => Type = attr {
    r =>
      if (regex(r).isEmpty)
        NothingType()
      else {
        val groups = regex(r).get.regexGroups
        assert(groups.nonEmpty)
        if (groups.length == 1 && groups.head.idn.isEmpty)
        // If there is a single group in the regex and the group doesn't have a name, do not create a record.
          StringType()
        else {
          RecordType(Attributes(groups.zipWithIndex.map {
            case (raw.regex.Regex.RegexGroup(_, Some(idn)), _) => AttrType(idn, StringType())
            case (raw.regex.Regex.RegexGroup(_, None), idx) => AttrType(s"_${idx + 1}", StringType())
          }))
        }
      }
  }

  lazy val scalaRegex: RegexConst => Option[String] = attr {
    r =>
      if (regex(r).isDefined)
        Some(regex(r).get.scalaRegex)
      else
        None
  }

}
