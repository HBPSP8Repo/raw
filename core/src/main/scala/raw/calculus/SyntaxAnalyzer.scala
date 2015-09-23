package raw
package calculus

import scala.util.matching.Regex
import scala.util.parsing.combinator.{RegexParsers, PackratParsers}
import scala.util.parsing.input.Positional

/** Parser for monoid comprehensions.
  */
object SyntaxAnalyzer extends RegexParsers with PackratParsers {

  import Calculus._
  import scala.collection.immutable.Seq

  override val whiteSpace =
    """(//.*\s*|\s+)+""".r

  /** Reserved words
    */
  // TODO: Make reserved words case-insensitive
  val kwOr = "or\\b".r
  val kwAnd = "and\\b".r
  val kwNot = "not\\b".r
  val kwUnion = "union\\b".r
  val kwBagUnion = "bag_union\\b".r
  val kwAppend = "append\\b".r
  val kwMax = "max\\b".r
  val kwSum = "sum\\b".r
  val kwMultiply = "multiply\\b".r
  val kwSet = "set\\b".r
  val kwBag = "bag\\b".r
  val kwList = "list\\b".r
  val kwNull = "null\\b".r
  val kwTrue = "true\\b".r
  val kwFalse = "false\\b".r
  val kwFor = "for\\b".r
  val kwYield = "yield\\b".r
  val kwIf = "if\\b".r
  val kwThen = "then\\b".r
  val kwElse = "else\\b".r
  val kwToBool = "to_bool\\b".r
  val kwToInt = "to_int\\b".r
  val kwToFloat = "to_float\\b".r
  val kwToString = "to_string\\b".r
  val kwGoTo = "go_to\\b".r
  val kwGoTo2 = "goto\\b".r
  val kwOrElse = "or_else\\b".r
  val kwSelect = "select\\b".r
  val kwDistinct = "distinct\\b".r
  val kwFrom = "from\\b".r
  val kwAs = "as\\b".r
  val kwIn = "in\\b".r
  val kwWhere = "where\\b".r
  val kwGroupBy = "group\\s+by\\b".r
  val kwOrder = "order\\b".r
  val kwHaving = "having\\b".r

  val reserved = kwOr | kwAnd | kwNot | kwUnion | kwBagUnion | kwAppend | kwMax | kwSum | kwMultiply | kwSet |
    kwBag | kwList | kwNull | kwTrue | kwFalse | kwFor | kwYield | kwIf | kwThen | kwElse | kwToBool | kwToInt |
    kwToFloat | kwToString | kwGoTo | kwGoTo2 | kwOrElse | kwSelect | kwDistinct | kwFrom | kwAs | kwIn | kwWhere |
    kwGroupBy | kwOrder | kwHaving

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[String, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }

  lazy val exp: PackratParser[Exp] =
    log(recordCons)("recordCons") |
    mergeExp

  lazy val recordCons: PackratParser[RecordCons] =
    log((attrCons <~ ",") ~ rep1sep(attrCons, ","))("recordCons #1") ^^ {
      case head ~ tail => RecordCons((Seq(head) ++ tail).zipWithIndex.map {
        case (att, idx) =>
          val natt = att.idn match {
            case Some(idn) => AttrCons(idn, att.e)
            case None      => AttrCons(s"_${idx + 1}", att.e)
          }
          natt.pos = att.pos
          natt
      })
    } |
    log(namedAttrCons)("recordCons #2") ^^ { case att => val natt = AttrCons(att.idn.head, att.e); natt.pos = att.pos; RecordCons(Seq(natt))}

  case class ParserAttrCons(e: Exp, idn: Option[Idn]) extends Positional

  lazy val attrCons: PackratParser[ParserAttrCons] =
    namedAttrCons |
    anonAttrCons

  lazy val namedAttrCons: PackratParser[ParserAttrCons] =
    positioned(
      log((attrName <~ ":") ~ mergeExp ^^ { case idn ~ e => ParserAttrCons(e, Some(idn)) })("namedAttrCons #1") |
      log(mergeExp ~ (kwAs ~> attrName) ^^ { case e ~ idn => ParserAttrCons(e, Some(idn)) })("namedAttrCons #2"))

  lazy val anonAttrCons: PackratParser[ParserAttrCons] =
    positioned(log(mergeExp)("anonAttrCons") ^^ { case e => ParserAttrCons(e, None) })

  lazy val mergeExp: PackratParser[Exp] =
    positioned(orExp * (monoidMerge ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val monoidMerge: PackratParser[Monoid] =
    positioned(
      kwUnion ^^^ SetMonoid() |
      kwBagUnion ^^^ BagMonoid() |
      kwAppend ^^^ ListMonoid() |
      kwMax ^^^ MaxMonoid())

  lazy val orExp: PackratParser[Exp] =
  positioned(andExp * (or ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val or: PackratParser[OrMonoid] =
    positioned(kwOr ^^^ OrMonoid())

  lazy val andExp: PackratParser[Exp] =
    positioned(comparisonExp * (and ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val and: PackratParser[AndMonoid] =
    positioned(kwAnd ^^^ AndMonoid())

  lazy val comparisonExp: PackratParser[Exp] =
    positioned(plusMinusExp * (comparisonOp ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val comparisonOp: PackratParser[ComparisonOperator] =
    positioned(
      "=" ^^^ Eq() |
      "<>" ^^^ Neq() |
      "!=" ^^^ Neq() |
      "<=" ^^^ Le() |
      "<" ^^^ Lt() |
      ">=" ^^^ Ge() |
      ">" ^^^ Gt())

  lazy val plusMinusExp: PackratParser[Exp] =
    positioned(minus ~ plusMinusExp ^^ { case op ~ e => UnaryExp(op, e)}) |
    "+" ~> plusMinusExp |
    termExp

  lazy val minus: PackratParser[Neg] =
    positioned("-" ^^^ Neg())

  lazy val termExp: PackratParser[Exp] =
    positioned(productExp * (
      sum ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      sub ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val sum: PackratParser[SumMonoid] =
    positioned("+" ^^^ SumMonoid())

  lazy val sub: PackratParser[Sub] =
    positioned("-" ^^^ Sub())

  lazy val productExp: PackratParser[Exp] =
    positioned(funAppExp * (
      mult ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      div ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val mult: PackratParser[MultiplyMonoid] =
    positioned("*" ^^^ MultiplyMonoid())

  lazy val div: PackratParser[Div] =
    positioned("/" ^^^ Div())

  lazy val funAppExp: PackratParser[Exp] =
    positioned((recordProjExp <~ "(") ~ (exp <~ ")") ^^ { case e1 ~ e2 => FunApp(e1, e2) }) |
    recordProjExp

  lazy val recordProjExp: PackratParser[Exp] =
    positioned(baseExp ~ ("." ~> repsep(attrName, ".")) ^^ { case e ~ idns =>
      def fold(e: Exp, idns: Seq[Idn]): Exp = idns match {
        case head :: Nil => RecordProj(e, idns.head)
        case head :: tail => fold(RecordProj(e, idns.head), idns.tail)
      }
      fold(e, idns)
      }) |
    baseExp

  lazy val attrName: PackratParser[String] =
    escapedIdent |
    ident

  lazy val escapedIdent: PackratParser[String] =
    """`[\w\t ]+`""".r ^^ {case s => s.drop(1).dropRight(1) }

  lazy val ident: PackratParser[String] =
    not(reserved) ~> """[_a-zA-Z]\w*""".r

  lazy val baseExp: PackratParser[Exp] =
    expBlock |
    const |
    ifThenElse |
    comp |
    select |
    consMonoid |
    zeroMonoid |
    unaryFun |
    funAbs |
    "(" ~> exp <~ ")" |
    idnExp

  lazy val expBlock: PackratParser[ExpBlock] =
    positioned(("{" ~> rep(bind <~ opt(";"))) ~ (exp <~ "}") ^^ { case b ~ e => ExpBlock(b, e) })

  lazy val const: PackratParser[Exp] =
    nullConst |
    boolConst |
    stringConst |
    numberConst

  lazy val nullConst: PackratParser[Null] =
    positioned("null" ^^^ Null())

  lazy val boolConst: PackratParser[BoolConst] =
    positioned(
      kwTrue ^^^ BoolConst(true) |
      kwFalse ^^^ BoolConst(false))

  lazy val stringConst: PackratParser[StringConst] =
    positioned(stringLit ^^ { case s => StringConst(s.drop(1).dropRight(1)) })

  lazy val stringLit =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r

  lazy val numberConst: PackratParser[Const] =
    positioned(
      numericLit ^^ { case v =>
        def isInt(s: String) = try { s.toInt; true } catch { case _: Throwable => false }
        if (isInt(v)) IntConst(v) else FloatConst(v)
      })

  lazy val numericLit =
    """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val ifThenElse: PackratParser[IfThenElse] =
    positioned(kwIf ~> exp ~ (kwThen ~> exp) ~ (kwElse ~> exp) ^^ { case e1 ~ e2 ~ e3 => IfThenElse(e1, e2, e3) })

  lazy val comp: PackratParser[Comp] =
  positioned((kwFor ~ "(") ~> (rep1sep(qualifier, ";") <~ ")") ~ (kwYield ~> monoid) ~ exp ^^ { case qs ~ m ~ e => Comp(m, qs, e) })

  lazy val monoid: PackratParser[Monoid] =
    primitiveMonoid |
    collectionMonoid |
    ident ~> err("illegal monoid")

  lazy val primitiveMonoid: PackratParser[PrimitiveMonoid] =
    positioned(
      kwSum ^^^ SumMonoid() |
      kwMultiply ^^^ MultiplyMonoid() |
      kwMax ^^^ MaxMonoid() |
      kwOr ^^^ OrMonoid() |
      kwAnd ^^^ AndMonoid())

  lazy val collectionMonoid: PackratParser[CollectionMonoid] =
    positioned(
      kwSet ^^^ SetMonoid() |
      kwBag ^^^ BagMonoid() |
      kwList ^^^ ListMonoid())

  lazy val qualifier: PackratParser[Qual] =
    gen |
    bind |
    filter

  lazy val gen: PackratParser[Gen] =
    positioned((pattern <~ "<-") ~ exp ^^ { case p ~ e => Gen(p, e) })

  lazy val pattern: PackratParser[Pattern] =
    "(" ~> patternProd <~ ")" |
    patternProd |
    patternIdn

  lazy val patternProd: PackratParser[Pattern] =
    positioned((pattern <~ ",") ~ rep1sep(pattern, ",") ^^ { case head ~ rest => PatternProd(head :: rest) })

  lazy val patternIdn: PackratParser[PatternIdn] =
    positioned(idnDef ^^ { case idn => PatternIdn(idn) })

  lazy val idnDef: PackratParser[IdnDef] =
    positioned(ident ^^ { case idn => IdnDef(idn) })

  lazy val bind: PackratParser[Bind] =
    positioned((pattern <~ ":=") ~ exp ^^ { case p ~ e => Bind(p, e) })

  lazy val filter: PackratParser[Exp] =
    exp


  lazy val select =
    positioned(kwSelect ~> distinct ~ exp ~ (kwFrom ~> iterators)
      ~ opt(kwWhere ~> exp) ~ opt(kwGroupBy ~> exp) ~ opt(kwOrder ~> exp) ~ opt(kwHaving ~> exp) ^^ {
      case d ~ proj ~ f ~ w ~ g ~ o ~ h => Select(f, d, proj, w, g, o, h)
    })

  // TODO: Handle ProjAttrCons pretty printer!

  lazy val distinct = opt(kwDistinct) ^^ { case e => e.isDefined }

  lazy val iterators = rep1sep(iterator, ",")

  lazy val iterator: PackratParser[Iterator] =
    positioned(ident ~ (kwIn ~> exp) ^^ { case i ~ e => Iterator(Some(PatternIdn(IdnDef(i))), e)}) |
      positioned(exp ~ ident ^^ { case e ~ i => Iterator(Some(PatternIdn(IdnDef(i))), e)}) |
      positioned(exp ^^ { case e => Iterator(None, e)})

  lazy val consMonoid: PackratParser[Exp] =
    collectionMonoid ~ ("(" ~> exp <~ ")") ^^ { case m ~ e => ConsCollectionMonoid(m, e) }

  lazy val zeroMonoid: PackratParser[Exp] =
    collectionMonoid <~ ("(" ~ ")") ^^ { case m => ZeroCollectionMonoid(m) }

  lazy val unaryFun: PackratParser[UnaryExp] =
    positioned(unaryOp ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => UnaryExp(op, e) })

  lazy val unaryOp: PackratParser[UnaryOperator] =
    positioned(
      kwToBool ^^^ ToBool() |
      kwToInt ^^^ ToInt() |
      kwToFloat ^^^ ToFloat() |
      kwToString ^^^ ToString())

  lazy val funAbs: PackratParser[FunAbs] =
    positioned("\\" ~> pattern ~ ("->" ~> exp) ^^ { case p ~ e => FunAbs(p, e) })

  lazy val notExp: PackratParser[UnaryExp] =
    positioned(not ~ exp ^^ { case op ~ e => UnaryExp(op, e)})

  lazy val not: PackratParser[Not] =
    positioned(kwNot ^^^ Not())

  lazy val idnExp: PackratParser[IdnExp] =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse: PackratParser[IdnUse] =
    positioned(ident ^^ IdnUse)

}