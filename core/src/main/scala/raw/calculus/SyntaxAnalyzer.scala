package raw
package calculus

import scala.util.parsing.combinator.{RegexParsers, PackratParsers}
import scala.util.parsing.input.Positional

// TODO: Add support for max, min, count, (avg?)
// TODO: Can't it be a stdlib that is always prefixed to a query?
// TODO: I don't think so. Is it a UnaryFun?

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
  val kwOr = "(?i)or\\b".r
  val kwAnd = "(?i)and\\b".r
  val kwNot = "(?i)not\\b".r
  val kwUnion = "(?i)union\\b".r
  val kwBagUnion = "((?i)bag_union|(?i)bagUnion)\\b".r
  val kwAppend = "(?i)append\\b".r
  val kwMax = "(?i)max\\b".r
  val kwSum = "(?i)sum\\b".r
  val kwMultiply = "(?i)multiply\\b".r
  val kwSet = "(?i)set\\b".r
  val kwBag = "(?i)bag\\b".r
  val kwList = "(?i)list\\b".r
  val kwNull = "(?i)null\\b".r
  val kwTrue = "(?i)true\\b".r
  val kwFalse = "(?i)false\\b".r
  val kwFor = "(?i)for\\b".r
  val kwYield = "(?i)yield\\b".r
  val kwIf = "(?i)if\\b".r
  val kwThen = "(?i)then\\b".r
  val kwElse = "(?i)else\\b".r
  val kwToBool = "((?i)to_bool|(?i)toBool)\\b".r
  val kwToInt = "((?i)to_int|(?i)toInt)\\b".r
  val kwToFloat = "((?i)to_float|(?i)toFloat)\\b".r
  val kwToString = "((?i)to_string|(?i)toString)\\b".r
  val kwToBag = "((?i)to_bag|(?i)toBag)\\b".r
  val kwToList = "((?i)to_list|(?i)toList)\\b".r
  val kwToSet = "((?i)to_set|(?i)toSet)\\b".r
  val kwAvg = "(?i)avg\\b".r
  val kwCount = "(?i)count\\b".r
  val kwMin = "(?i)min\\b".r
  val kwGoTo = "((?i)go_to|(?i)goto|(?i)goTo)\\b".r
  val kwGetOrElse = "((?i)get_or_else|(?i)getOrElse)\\b".r
  val kwOrElse = "((?i)or_else|(?i)orElse)\\b".r
  val kwBreak = "(?i)break\\b".r
  val kwContinue = "(?i)continue\\b".r
  val kwSelect = "(?i)select\\b".r
  val kwDistinct = "(?i)distinct\\b".r
  val kwFrom = "(?i)from\\b".r
  val kwAs = "(?i)as\\b".r
  val kwIn = "(?i)in\\b".r
  val kwExists = "(?i)exists\\b".r
  val kwWhere = "(?i)where\\b".r
  val kwGroup = "(?i)group\\b".r
  val kwOrder = "(?i)order\\b".r
  val kwBy = "(?i)by\\b".r
  val kwHaving = "(?i)having\\b".r
  val kwPartition = "(?i)partition\\b".r
  val kwTry = "(?i)try\\b".r
  val kwCatch = "(?i)catch\\b".r
  val kwExcept = "(?i)except\\b".r
  val kwNew = "(?i)new\\b".r
  val kwType = "(?i)type\\b".r
  val kwAlias = "(?i)alias\\b".r
  val kwStar = "*"

  // TODO: Alphabetic order
  // TODO: Add more general support for built-in functions(?)

  val reserved = kwOr | kwAnd | kwNot | kwUnion | kwBagUnion | kwAppend | kwMax | kwSum | kwMultiply | kwSet |
    kwBag | kwList | kwNull | kwTrue | kwFalse | kwFor | kwYield | kwIf | kwThen | kwElse | kwToBool | kwToInt |
    kwToFloat | kwToString | kwToBag | kwToList | kwToSet | kwAvg | kwCount | kwMin | kwGoTo | kwGetOrElse | kwOrElse |
    kwBreak| kwContinue | kwSelect | kwDistinct | kwFrom | kwAs | kwIn | kwWhere | kwGroup | kwOrder | kwBy | kwHaving |
    kwPartition | kwTry | kwCatch | kwExcept | kwNew | kwType | kwAlias | kwStar

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[String, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }

  lazy val exp: PackratParser[Exp] =
    recordCons |
    mergeExp

  lazy val recordCons: PackratParser[RecordCons] =
    (attrCons <~ ",") ~ rep1sep(attrCons, ",") ^^ {
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
    namedAttrCons ^^ { case att => val natt = AttrCons(att.idn.head, att.e); natt.pos = att.pos; RecordCons(Seq(natt))}

  case class ParserAttrCons(e: Exp, idn: Option[Idn]) extends Positional

  lazy val attrCons: PackratParser[ParserAttrCons] =
    namedAttrCons |
    anonAttrCons

  lazy val namedAttrCons: PackratParser[ParserAttrCons] =
    positioned(
      (attrName <~ ":") ~ mergeExp ^^ { case idn ~ e => ParserAttrCons(e, Some(idn)) } |
      mergeExp ~ (kwAs ~> attrName) ^^ { case e ~ idn => ParserAttrCons(e, Some(idn)) })

  lazy val anonAttrCons: PackratParser[ParserAttrCons] =
    positioned(mergeExp ^^ {
      case e => e match {
        case RecordProj(_, field) => ParserAttrCons(e, Some(field))
        case IdnExp(IdnUse(x)) => ParserAttrCons(e, Some(x))
        case _ => ParserAttrCons(e, None)
      }
    })

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
    positioned(inExp * (comparisonOp ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val comparisonOp: PackratParser[ComparisonOperator] =
    positioned(
      "=" ^^^ Eq() |
      "<>" ^^^ Neq() |
      "!=" ^^^ Neq() |
      "<=" ^^^ Le() |
      "<" ^^^ Lt() |
      ">=" ^^^ Ge() |
      ">" ^^^ Gt())

  lazy val inExp: PackratParser[Exp] =
    positioned(plusMinusExp * ("in" ^^^ { (e1: Exp, e2: Exp) => InExp(e1, e2) }))

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
    multiCons |
    unaryFun |
    sugarFun |
    funAbs |
    partition |
    notExp |
    starExp |
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
    positioned((opt(pattern) <~ "<-") ~ exp ^^ { case p ~ e => Gen(p, e) })

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
      ~ opt(kwWhere ~> exp) ~ opt(kwGroup ~> kwBy ~> exp) ~ opt(kwOrder ~> kwBy ~> exp) ~ opt(kwHaving ~> exp) ^^ {
      case d ~ proj ~ f ~ w ~ g ~ o ~ h => Select(f, d, g, proj, w, o, h)
    })

  lazy val distinct = opt(kwDistinct) ^^ { case e => e.isDefined }

  lazy val iterators = rep1sep(iterator, ",")

  lazy val iterator: PackratParser[Gen] =
    positioned(
      ident ~ (kwIn ~> mergeExp) ^^ { case i ~ e => Gen(Some(PatternIdn(IdnDef(i))), e)} |
      mergeExp ~ (kwAs ~> ident) ^^ { case e ~ i => Gen(Some(PatternIdn(IdnDef(i))), e)} |
      mergeExp ~ ident ^^ { case e ~ i => Gen(Some(PatternIdn(IdnDef(i))), e)} |
      mergeExp ^^ { case e => Gen(None, e)})

  lazy val multiCons: PackratParser[MultiCons] =
    positioned(collectionMonoid ~ ("(" ~> repsep(mergeExp, ",") <~ ")") ^^ { case m ~ es => MultiCons(m, es) })

  lazy val unaryFun: PackratParser[UnaryExp] =
    positioned(unaryOp ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => UnaryExp(op, e) })

  lazy val unaryOp: PackratParser[UnaryOperator] =
    positioned(
      kwToBool ^^^ ToBool() |
      kwToInt ^^^ ToInt() |
      kwToFloat ^^^ ToFloat() |
      kwToString ^^^ ToString() |
      kwToBag ^^^ ToBag() |
      kwToList ^^^ ToList() |
      kwToSet ^^^ ToSet())

  lazy val sugarFun: PackratParser[Sugar] =
    positioned(
      kwSum ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Sum(e) } |
      kwMax ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Max(e) } |
      kwMin ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Min(e) } |
      kwAvg ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Avg(e) } |
      kwCount ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Count(e) } |
      kwExists ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Exists(e) })

  lazy val funAbs: PackratParser[FunAbs] =
    positioned("\\" ~> pattern ~ ("->" ~> exp) ^^ { case p ~ e => FunAbs(p, e) })

  lazy val partition: PackratParser[Partition] =
    kwPartition ^^^ Partition()

  lazy val notExp: PackratParser[UnaryExp] =
    positioned(not ~ exp ^^ { case op ~ e => UnaryExp(op, e)})

  lazy val not: PackratParser[Not] =
    positioned(kwNot ^^^ Not())

  lazy val starExp: PackratParser[Star] =
    positioned(kwStar ^^^ Star())

  lazy val idnExp: PackratParser[IdnExp] =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse: PackratParser[IdnUse] =
    positioned(ident ^^ IdnUse)

}