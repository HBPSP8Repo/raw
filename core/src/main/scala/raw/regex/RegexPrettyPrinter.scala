package raw.regex

import org.kiama.output.PrettyPrinterTypes.Width
import raw.PrettyPrinter

object RegexPrettyPrinter extends PrettyPrinter {

  def apply(n: RawRegex, w: Width = 120): String =
    super.pretty(show(n), w=w).layout

  def show(n: RawRegex): Doc = group(n match {
    case RegexSeq(rs) => ssep(rs.map(show).to, "")
    case RegexPipe(rs) => ssep(rs.map(show).to, "|")
    case RegexWord() => "\\w"
    case RegexSpace() => "\\s"
    case RegexDigit() => "\\d"
    case RegexWildcard() => "."
    case RegexChar(c) => c
    case RegexEscapedChar(c) => "\\" <> c
    case RegexPlus(r) => show(r) <> "+"
    case RegexQuestion(r) => show(r) <> "?"
    case RegexStar(r) => show(r) <> "*"
    case RegexRepeat(r, n1) => show(r) <> "{" <> n1.toString <> "}"
    case RegexBeginLine() => "^"
    case RegexEndLine() => "$"
    case RegexOr(ors, not) => (if (not) "[^" else "[") <> ssep(ors.map{
      case RegexOrInterval(a, b) => text(s"$a-$b")
      case RegexOrPrimitive(p) => show(p)
      }.to, "") <> "]"
    case RegexGroup(r, Some(idn)) => "(" <> idn <> ":" <> show(r) <> ")"
    case RegexGroup(r, None) => "(" <> show(r) <> ")"
  })
   
}