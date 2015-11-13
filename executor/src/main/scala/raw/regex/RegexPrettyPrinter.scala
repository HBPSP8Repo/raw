package raw
package regex
import org.kiama.output.PrettyPrinterTypes.Width

object RegexPrettyPrinter extends PrettyPrinter {

  import Regex._

  def apply(n: RegexNode, w: Width = 120, scala: Boolean = false): String =
    super.pretty(show(n, scala), w=w).layout

  def show(n: RegexNode, scala: Boolean): Doc = group(n match {
    case RegexSeq(rs) => ssep(rs.map(r => show(r, scala)).to, "")
    case RegexPipe(rs) => ssep(rs.map(r => show(r, scala)).to, "|")
    case RegexWord() => "\\w"
    case RegexSpace() => "\\s"
    case RegexDigit() => "\\d"
    case RegexWildcard() => "."
    case RegexChar(c) => c
    case RegexEscapedChar(c) => "\\" <> c
    case RegexPlus(r) => show(r, scala) <> "+"
    case RegexQuestion(r) => show(r, scala) <> "?"
    case RegexStar(r) => show(r, scala) <> "*"
    case RegexRepeat(r, n1) => show(r, scala) <> "{" <> n1.toString <> "}"
    case RegexBeginLine() => "^"
    case RegexEndLine() => "$"
    case RegexOr(ors, not) => (if (not) "[^" else "[") <> ssep(ors.map{
      case RegexOrInterval(a, b) => text(s"$a-$b")
      case RegexOrPrimitive(p) => show(p, scala)
      }.to, "") <> "]"
    case RegexGroup(r, Some(idn)) if !scala => "(" <> idn <> ":" <> show(r, scala) <> ")"
    case RegexGroup(r, _) => "(" <> show(r, scala) <> ")"
  })
   
}
