package raw
package calculus

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.{CharSequenceReader, Positional}

/** Parser for monoid comprehensions.
  */
trait BaseSyntaxAnalyzer extends RegexParsers with PackratParsers {

  import Calculus._

  import scala.collection.immutable.Seq

  override val whiteSpace =
    """(//.*\s*|\s+)+""".r

  /** List of reserved keywords in alphabetic order.
    * See docs below on reserved keywords groups for more details on how to add a reserved keyword.
    */
  val kwAnd = "(?i)and\\b".r
  val kwAppend = "(?i)append\\b".r
  val kwAs = "(?i)as\\b".r
  val kwAvg = "(?i)avg\\b".r
  val kwBag = "(?i)bag\\b".r
  val kwBagUnion = "((?i)bag_union|(?i)bagUnion)\\b".r
  val kwBool = "(?i)bool\\b".r
  val kwBreak = "(?i)break\\b".r
  val kwBy = "(?i)by\\b".r
  val kwCatch = "(?i)catch\\b".r
  val kwContinue = "(?i)continue\\b".r
  val kwCount = "(?i)count\\b".r
  val kwDateTime = "(?i)datetime\\b".r
  val kwDistinct = "(?i)distinct\\b".r
  val kwElse = "(?i)else\\b".r
  val kwExcept = "(?i)except\\b".r
  val kwExists = "(?i)exists\\b".r
  val kwFail = "(?i)fail\\b".r
  val kwFinally = "(?i)finally\\b".r
  val kwFor = "(?i)for\\b".r
  val kwFrom = "(?i)from\\b".r
  val kwGoTo = "((?i)go_to|(?i)goto|(?i)goTo)\\b".r
  val kwGroup = "(?i)group\\b".r
  val kwHaving = "(?i)having\\b".r
  val kwIf = "(?i)if\\b".r
  val kwIn = "(?i)in\\b".r
  val kwInt = "(?i)int\\b".r
  val kwInterval = "(?i)interval\\b".r
  val kwInto = "(?i)into\\b".r
  val kwIs = "(?i)is\\b".r
  val kwLike = "(?i)in\\b".r
  val kwList = "(?i)list\\b".r
  val kwFalse = "(?i)false\\b".r
  val kwFloat = "(?i)float\\b".r
  val kwIsNull = "((?i)is_null|(?i)isNull)\\b".r
  val kwMax = "(?i)max\\b".r
  val kwMin = "(?i)min\\b".r
  val kwMultiply = "(?i)multiply\\b".r
  val kwNew = "(?i)new\\b".r
  val kwNot = "(?i)not\\b".r
  val kwNull = "(?i)null\\b".r
  val kwOn = "(?i)on\\b".r
  val kwOption = "(?i)option\\b".r
  val kwOr = "(?i)or\\b".r
  val kwOrder = "(?i)order\\b".r
  val kwPartition = "(?i)partition\\b".r
  val kwParse = "(?i)parse\\b".r
  val kwRecord = "(?i)record\\b".r
  val kwRegex = "(?i)regex\\b".r
  val kwSelect = "(?i)select\\b".r
  val kwSet = "(?i)set\\b".r
  val kwSkip = "(?i)skip\\b".r
  val kwString = "(?i)string\\b".r
  val kwSum = "(?i)sum\\b".r
  val kwThen = "(?i)then\\b".r
  val kwToBag = "((?i)to_bag|(?i)toBag)\\b".r
  val kwToBool = "((?i)to_bool|(?i)toBool)\\b".r
  val kwToEpoch = "((?i)to_epoch|(?i)toEpoch)\\b".r
  val kwToFloat = "((?i)to_float|(?i)toFloat)\\b".r
  val kwToList = "((?i)to_list|(?i)toList)\\b".r
  val kwToInt = "((?i)to_int|(?i)toInt)\\b".r
  val kwToRegex = "((?i)to_regex|(?i)toRegex)\\b".r
  val kwToSet = "((?i)to_set|(?i)toSet)\\b".r
  val kwToString = "((?i)to_string|(?i)toString)\\b".r
  val kwTrue = "(?i)true\\b".r
  val kwTry = "(?i)try\\b".r
  val kwType = "(?i)type\\b".r
  val kwUnion = "(?i)union\\b".r
  val kwWhere = "(?i)where\\b".r
  val kwYield = "(?i)yield\\b".r

  // DateTime/Timestamp/Interval keywords
  //  val kwDate = "(?i)date\\b".r
  //  val kwTime = "(?i)time\\b".r
  //  val kwInterval = "(?i)interval\\b".r
  //  val kwTimestamp = "(?i)timestamp\\b".r
  //  val kwWith = "(?i)with\\b".r
  //  val kwWithout = "(?i)without\\b".r
  //  val kwYear = "(?i)year\\b".r
  //  val kwMonth = "(?i)month\\b".r
  //  val kwDay = "(?i)day\\b".r
  //  val kwHour = "(?i)hour\\b".r
  //  val kwMinute = "(?i)minute\\b".r
  //  val kwSecond = "(?i)second\\b".r
  //  val kwTo = "(?i)to\\b".r

  /** Reserved keywords organized by "group".
    * The reserved keywords are split per "group" instead of all in a "large chunk" so that, in the future, we can adapt
    * the parser to only "deny" the use of reserved keywords that are truly impossible/conflicting in that particular
    * parsing context. That is to say: not all reserved keywords need to be denied globally, in all occasions!
    * That's also the reason a particular keyword may be repeated in more than one group: e.g. kwSum may be used as
    * a monoid or as a built-in [sugar] function.
    *
    * To add a reserved keyword:
    * 1) Add it first to the set of reserved keywords above, which is sorted in alphabetic order.
    *    This is not enough however; then you need to:
    * 2) Go through the list of "reserved keyword groups" defined below and add it also there if it makes sense: again,
    *    the keywords in each group are sorted in alphabetic order, so pls keep that order!.
    */
  val reservedConstants = kwFalse | kwNull | kwTrue
  val reservedTypes = kwBag | kwBool | kwDateTime | kwFloat | kwInt | kwInterval | kwList | kwOption | kwRecord |
    kwRegex | kwSet | kwString
  val reservedMonoids = kwAnd | kwBag | kwList | kwMax | kwMultiply | kwOr | kwSet | kwSum
  val reservedBinaryOps = kwAnd | kwAppend | kwBagUnion | kwIn | kwIs | kwLike | kwMax | kwMin | kwMultiply | kwNot |
    kwNull | kwOr | kwUnion
  val reservedComp = kwFor | kwYield
  val reservedSelect = kwAs | kwBy | kwDistinct | kwFail | kwFrom | kwGroup | kwHaving | kwInto | kwOn | kwOrder |
    kwParse | kwPartition | kwSelect | kwSkip | kwWhere
  val reservedControlFlow = kwElse | kwIf | kwThen
  val reservedFuture = kwBreak | kwCatch | kwContinue | kwExcept | kwFinally | kwGoTo | kwNew | kwTry | kwType
  val reservedTypeConv = kwToBag | kwToBool | kwToEpoch | kwToFloat | kwToInt | kwToList | kwToSet | kwToString |
    kwToRegex
  val reservedBuiltIn = kwAvg | kwCount | kwExists | kwIsNull | kwMin

  val reserved = reservedConstants | reservedTypes | reservedMonoids | reservedBinaryOps | reservedComp |
    reservedSelect | reservedControlFlow | reservedFuture | reservedTypeConv | reservedBuiltIn

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[NoSuccess, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case error: NoSuccess => Left(error)
  }

  /** Work-around for parser combinator bug.
    */
  def parseSubstring[T](parser: Parser[T], input: String): ParseResult[T] = {
    parse(parser, new PackratReader(new CharSequenceReader(input)))
  }

  /** Parser combinators.
    */

  lazy val exp: PackratParser[Exp] =
    recordCons |
    mergeExp

  lazy val recordCons: PackratParser[RecordCons] =
    positioned(
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
    )
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
        case _: Partition => ParserAttrCons(e, Some("partition"))
        case IdnExp(IdnUse(x)) => ParserAttrCons(e, Some(x))
        case _ => ParserAttrCons(e, None)
      }
    })

  lazy val mergeExp: PackratParser[Exp] =
    positioned(orExp * (monoidMerge ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val monoidMerge: PackratParser[BinaryOperator] =
    positioned(
      kwUnion ^^^ Union() |
      kwBagUnion ^^^ BagUnion() |
      kwAppend ^^^ Append())

  lazy val orExp: PackratParser[Exp] =
  positioned(andExp * (or ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val or: PackratParser[BinaryOperator] =
    positioned(kwOr ^^^ Or())

  lazy val andExp: PackratParser[Exp] =
    positioned(comparisonExp * (and ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val and: PackratParser[BinaryOperator] =
    positioned(kwAnd ^^^ And())

  lazy val comparisonExp: PackratParser[Exp] =
    positioned(inExp * (comparisonOp ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val comparisonOp: PackratParser[BinaryOperator] =
    positioned(
      "=" ^^^ Eq() |
      "<>" ^^^ Neq() |
      "!=" ^^^ Neq() |
      "<=" ^^^ Le() |
      "<" ^^^ Lt() |
      ">=" ^^^ Ge() |
      ">" ^^^ Gt() |
      kwLike ^^^ Like() |
      kwNot ~ kwLike ^^^ NotLike() |
      kwIn ^^^ In() |
      kwNot ~ kwIn ^^^ NotIn() |
      kwIs ~ kwNull ^^^ IsNullOp() |
      kwIs ~ kwNot ~ kwNull ^^^ IsNotNull() )

  lazy val inExp: PackratParser[Exp] =
    positioned(intoExp * (kwIn ^^^ { (e1: Exp, e2: Exp) => InExp(e1, e2) }))

  lazy val intoExp: PackratParser[Exp] =
    positioned(plusMinusExp * (kwInto ^^^ { (e1: Exp, e2: Exp) => Into(e1, e2) }))

  lazy val plusMinusExp: PackratParser[Exp] =
    positioned(minus ~ plusMinusExp ^^ { case op ~ e => UnaryExp(op, e)}) |
    "+" ~> plusMinusExp |
    termExp

  lazy val minus: PackratParser[Neg] =
    positioned("-" ^^^ Neg())

  lazy val termExp: PackratParser[Exp] =
    positioned(productExp * (
      sum ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } |
      sub ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val sum: PackratParser[Plus] =
    positioned("+" ^^^ Plus())

  lazy val sub: PackratParser[Sub] =
    positioned("-" ^^^ Sub())

  lazy val productExp: PackratParser[Exp] =
    positioned(funAppExp * (
      mult ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } |
      div ^^ { case op => (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) }))

  lazy val mult: PackratParser[Mult] =
    positioned("*" ^^^ Mult())

  lazy val div: PackratParser[Div] =
    positioned("/" ^^^ Div())

  lazy val funAppExp: PackratParser[Exp] =
    positioned((parseAsExp <~ "(") ~ (rep1sep(mergeExp, ",") <~ ")") ^^ { case f ~ args => FunApp(f, args) }) |
    parseAsExp

  lazy val parseAsExp: PackratParser[Exp] =
    positioned(
      (recordProjExp <~ kwParse <~ kwAs) ~ regexConst ~ parseProperties ^^ { case e ~ r ~ p => ParseAs(e, r, Some(p))} |
      (recordProjExp <~ kwParse <~ kwAs) ~ regexConst ^^ { case e ~ r => ParseAs(e, r, None)} |
      recordProjExp)

  lazy val parseProperties: PackratParser[ParseProperties] =
    skipOnFail |
    nullOnFail

  lazy val skipOnFail: PackratParser[SkipOnFail] =
    kwSkip ~ kwOn ~ kwFail ^^^ SkipOnFail()

  lazy val nullOnFail: PackratParser[NullOnFail] =
    kwNull ~ kwOn ~ kwFail ^^^ NullOnFail()

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
    isNullFun |
    toEpochFun |
    sugarFun |
    notExp |
//    namedFun |  -> Not an expression; must go into ExpBlock
    funAbs |
    partition |
    starExp |
    "(" ~> exp <~ ")" |
    idnExp

  lazy val expBlock: PackratParser[ExpBlock] =
    positioned(("{" ~> rep(bind <~ opt(";"))) ~ (exp <~ "}") ^^ { case b ~ e => ExpBlock(b, e) })

  lazy val const: PackratParser[Exp] =
    nullConst |
    boolConst |
    stringConst |
    numberConst |
    regexConst

  lazy val nullConst: PackratParser[Null] =
    positioned(kwNull ^^^ Null())

  lazy val boolConst: PackratParser[BoolConst] =
    positioned(
      kwTrue ^^^ BoolConst(true) |
      kwFalse ^^^ BoolConst(false))

  private def escapeStr(s: String) = {
    var escapedStr = ""
    var escape = false
    for (c <- s) {
      if (!escape) {
        if (c == '\\') {
          escape = true
        } else {
          escapedStr += c
        }
      } else {
        escapedStr += (c match {
          case '\\' => '\\'
          case ''' => '''
          case '"' => '"'
          case 'b' => '\b'
          case 'f' => '\f'
          case 'n' => '\n'
          case 'r' => '\r'
          case 't' => '\t'
        })
        escape = false
      }
    }
    escapedStr
  }

  lazy val stringConst: PackratParser[StringConst] =
    positioned(stringLit ^^ { case s => StringConst(escapeStr(s).drop(1).dropRight(1)) })

  lazy val stringLit =
    """"([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*"""".r

  lazy val numberConst: PackratParser[Const] =
    positioned(
      numericLit ^^ { case v =>
        def isInt(s: String) = try { s.toInt; true } catch { case _: Throwable => false }
        if (isInt(v)) IntConst(v) else FloatConst(v)
      })

  lazy val numericLit =
    """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val regexConst: PackratParser[RegexConst] =
    positioned(regexLitTriple ^^ { case r => RegexConst(r.drop(4).dropRight(3)) }) |
    positioned(regexLit ^^ { case r => RegexConst(escapeStr(r).drop(2).dropRight(1)) }) |
    """r"""".r ~> err("invalid regular expression")

  lazy val regexLit =
    """r"([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*"""".r

  lazy val regexLitTriple =
    "r\"\"\".*\"\"\"".r

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
    positioned(
      ident ~ (":" ~> tipe) ^^ { case idn ~ t => IdnDef(idn, Some(t)) } |
      ident ^^ { case idn => IdnDef(idn, None)} )

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
      idnDef ~ (kwIn ~> mergeExp) ^^ { case idn ~ e => Gen(Some(PatternIdn(idn)), e)} |
      mergeExp ~ (kwAs ~> idnDef) ^^ { case e ~ idn => Gen(Some(PatternIdn(idn)), e)} |
      mergeExp ~ idnDef ^^ { case e ~ idn => Gen(Some(PatternIdn(idn)), e)} |
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
      kwToList ^^^ ToList())

  lazy val isNullFun: PackratParser[IsNull] =
    positioned(kwIsNull ~> ("(" ~> mergeExp) ~ ("," ~> mergeExp <~ ")") ^^ { case e ~ o => IsNull(e, o) })

  lazy val toEpochFun: PackratParser[ToEpoch] =
    positioned(kwToEpoch ~> ("(" ~> mergeExp) ~ ("," ~> stringLit <~ ")") ^^ { case e ~ fmt => ToEpoch(e, fmt.drop(1).dropRight(1)) })

  lazy val sugarFun: PackratParser[Sugar] =
    sumExp |
    maxExp |
    minExp |
    avgExp |
    countExp |
    existsExp

  lazy val sumExp: PackratParser[Sum] =
    positioned(kwSum ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Sum(e) })

  lazy val maxExp: PackratParser[Max] =
    positioned(kwMax ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Max(e) })

  lazy val minExp: PackratParser[Min] =
    positioned(kwMin ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Min(e) })

  lazy val avgExp: PackratParser[Avg] =
    positioned(kwAvg ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Avg(e) })

  lazy val countExp: PackratParser[Count] =
    positioned(kwCount ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Count(e) })

  lazy val existsExp: PackratParser[Exists] =
    positioned(kwExists ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => Exists(e) })

//  lazy val namedFun: PackratParser[Bind] =
//    positioned(patternIdn ~ namedFunAbs ^^ { case idn ~ f => Bind(idn, f) })
//
//  lazy val namedFunAbs: PackratParser[FunAbs] =
//    positioned(pattern ~ (":=" ~> exp) ^^ { case p ~ e => FunAbs(p, e) })

  lazy val funAbs: PackratParser[FunAbs] =
    positioned(
      "\\" ~> "(" ~> rep1sep(idnDef, ",") ~ (")" ~> "->" ~> exp) ^^ { case args ~ e => FunAbs(args, e) } |
      "\\" ~> rep1sep(idnDef, ",") ~ ("->" ~> exp) ^^ { case args ~ e => FunAbs(args, e) })

  lazy val partition: PackratParser[Partition] =
    positioned(kwPartition ^^^ Partition())

  lazy val notExp: PackratParser[UnaryExp] =
    positioned(not ~ exp ^^ { case op ~ e => UnaryExp(op, e)})

  lazy val not: PackratParser[Not] =
    positioned(kwNot ^^^ Not())

  lazy val starExp: PackratParser[Star] =
    positioned("*" ^^^ Star())

  lazy val typeAs: PackratParser[TypeAs] =
  // TODO: IS?
    positioned((exp <~ kwType) ~ (kwAs ~> tipe) ^^ { case e ~ t => TypeAs(e, t)} )

  lazy val tipe: PackratParser[Type] =
    positioned(
      kwBool ^^^ BoolType() |
      kwInt ^^^ IntType() |
      kwFloat ^^^ FloatType() |
      kwString ^^^ StringType() |
      kwDateTime ^^^ DateTimeType(true) |
      kwRegex ^^^ RegexType() |
      kwOption ~ "(" ~> tipe <~ ")" ^^ OptionType |
      kwRecord ~ "(" ~> rep1sep(attrType, ",") <~ ")" ^^ { case atts => RecordType(Attributes(atts)) } |
      kwSet ~ "(" ~> tipe <~ ")" ^^ (CollectionType(SetMonoid(), _)) |
      kwBag ~ "(" ~> tipe <~ ")" ^^ (CollectionType(BagMonoid(), _)) |
      kwList ~ "(" ~> tipe <~ ")" ^^ (CollectionType(ListMonoid(), _)) |
      ident ^^ { case idn => UserType(Symbol(idn)) } |
      failure("illegal type"))

  lazy val attrType: PackratParser[AttrType] =
    positioned((ident <~ ":") ~ tipe ^^ { case idn ~ t => AttrType(idn, t) })

  lazy val idnExp: PackratParser[IdnExp] =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse: PackratParser[IdnUse] =
    positioned(ident ^^ IdnUse)

}

/** The default syntax analyzer.
 */
object SyntaxAnalyzer extends BaseSyntaxAnalyzer

/** A syntax analyzer that parsers identifiers such as $1.
  * It is used by test cases, to parse "expected results".
  */
object DebugSyntaxAnalyzer extends BaseSyntaxAnalyzer {

  override lazy val ident: PackratParser[String] =
    not(reserved) ~> """[_a-zA-Z0-9$]+""".r

}