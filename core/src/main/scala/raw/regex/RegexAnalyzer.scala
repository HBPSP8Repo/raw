package raw
package regex

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

class RegexAnalyzer(val tree: Regex.Regex) extends Attribution with LazyLogging {

  import scala.collection.immutable.Seq
  import Regex._

  private lazy val errorInScala: Option[String] = {
    val re = RegexPrettyPrinter(tree.root)
    try {
      new scala.util.matching.Regex(re)
      None
    } catch {
      case je: java.util.regex.PatternSyntaxException => Some(je.getMessage)
      case e: Exception => Some(e.getMessage)
    }
  }

  lazy val regexErrors: Seq[RegexError] =
    if (errorInScala.isDefined)
      Seq(InvalidRegex(errorInScala.get))
    else
      Seq()

  private lazy val groups: RegexNode => Seq[RegexGroup] = attr {
    case RegexSeq(rs) => rs.flatMap(groups)
    case RegexPipe(rs) => rs.flatMap(groups)
    case RegexWord() => Seq()
    case RegexSpace() => Seq()
    case RegexDigit() => Seq()
    case RegexWildcard() => Seq()
    case RegexChar(c) => Seq()
    case RegexEscapedChar(c) => Seq()
    case RegexPlus(r) => groups(r)
    case RegexQuestion(r) => groups(r)
    case RegexStar(r) => groups(r)
    case RegexRepeat(r, _) => groups(r)
    case RegexBeginLine() => Seq()
    case RegexEndLine() => Seq()
    case RegexOr(ors, not) => ors.flatMap{
      case RegexOrInterval(a, b) => Seq()
      case RegexOrPrimitive(p) => groups(p)
    }
    case n1 @ RegexGroup(r, _) => Seq(n1) ++ groups(r)
  }

  lazy val regexGroups = groups(tree.root)

}
