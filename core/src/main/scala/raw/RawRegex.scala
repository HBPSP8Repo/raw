package raw

sealed abstract class RawRegex extends RawNode

sealed abstract class RawPrimitiveRegex extends RawRegex

case class RegexSeq(rs: Seq[RawRegex]) extends RawRegex

/** ... | ... */
case class RegexPipe(rs: Seq[RawRegex]) extends RawRegex

/** \w */
case class RegexWord() extends RawPrimitiveRegex

/** \s */
case class RegexSpace() extends RawPrimitiveRegex

/** \d */
case class RegexDigit() extends RawPrimitiveRegex

/** . */
case class RegexWildcard() extends RawPrimitiveRegex

/** char */
case class RegexChar(c: Char) extends RawPrimitiveRegex

/** + */
case class RegexPlus(r: RawRegex) extends RawRegex

/** ? */
case class RegexQuestion(r: RawRegex) extends RawRegex

/** * */
case class RegexStar(r: RawRegex) extends RawRegex

/** { } */
case class RegexRepeat(r: RawRegex, n: Int) extends RawRegex

/** ^ */
case class RegexBeginLine() extends RawRegex

/** $ */
case class RegexEndLine() extends RawRegex

/** [] */

sealed abstract class RegexOrContent extends RawNode

case class RegexOrInterval(a: Char, b: Char) extends RegexOrContent

case class RegexOrPrimitive(p: RawPrimitiveRegex) extends RegexOrContent

case class RegexOr(ors: Seq[RegexOrContent], not: Boolean) extends RawRegex

/** (foo: ...) */

case class RegexGroup(r: RawRegex, idn: Option[String]) extends RawRegex
