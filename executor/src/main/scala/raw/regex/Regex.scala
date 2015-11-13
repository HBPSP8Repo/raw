package raw
package regex


/** Regex
  */
object Regex {

  import org.kiama.relation.Tree

  import scala.collection.immutable.Seq

  /** Tree type for Regex
    */
  type Regex = Tree[RawNode,RegexNode]

  /** Identifiers are represented as strings
    */
  type Idn = String

  /** Base class for all Regex nodes.
    */
  sealed abstract class RegexNode extends RawNode

  sealed abstract class PrimitiveRegexNode extends RegexNode

  case class RegexSeq(rs: Seq[RegexNode]) extends RegexNode

  /** ... | ... */
  case class RegexPipe(rs: Seq[RegexNode]) extends RegexNode

  /** \w */
  case class RegexWord() extends PrimitiveRegexNode

  /** \s */
  case class RegexSpace() extends PrimitiveRegexNode

  /** \d */
  case class RegexDigit() extends PrimitiveRegexNode

  /** . */
  case class RegexWildcard() extends PrimitiveRegexNode

  /** char */
  case class RegexChar(c: Char) extends PrimitiveRegexNode
  case class RegexEscapedChar(c: Char) extends PrimitiveRegexNode

  /** + */
  case class RegexPlus(r: RegexNode) extends RegexNode

  /** ? */
  case class RegexQuestion(r: RegexNode) extends RegexNode

  /** * */
  case class RegexStar(r: RegexNode) extends RegexNode

  /** { } */
  case class RegexRepeat(r: RegexNode, n: Int) extends RegexNode

  /** ^ */
  case class RegexBeginLine() extends RegexNode

  /** $ */
  case class RegexEndLine() extends RegexNode

  /** [] */

  sealed abstract class RegexOrContent extends RawNode

  case class RegexOrInterval(a: Char, b: Char) extends RegexOrContent

  case class RegexOrPrimitive(p: PrimitiveRegexNode) extends RegexOrContent

  case class RegexOr(ors: Seq[RegexOrContent], not: Boolean) extends RegexNode

  /** (foo: ...) */

  case class RegexGroup(r: RegexNode, idn: Option[String]) extends RegexNode

}