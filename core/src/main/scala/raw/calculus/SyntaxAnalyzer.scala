package raw
package calculus

import scala.util.parsing.combinator.{RegexParsers, PackratParsers}

/** Parser for monoid comprehensions.
  */
object SyntaxAnalyzer extends RegexParsers with PackratParsers {

  import Calculus._
  import scala.collection.immutable.List

//  override val whitespace =
//    regex("""(\s|(//.*\n))+""".r)

  /** Reserved words
    */
  // TODO: Make reserved words case-insensitive?
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
  val kwOrElse = "or_else\\b".r

  val reserved = kwOr | kwAnd | kwNot | kwUnion | kwBagUnion | kwAppend | kwMax | kwSum | kwMultiply | kwSet |
    kwBag | kwList | kwNull | kwTrue | kwFalse | kwFor | kwYield | kwIf | kwThen | kwElse | kwToBool | kwToInt |
    kwToFloat | kwToString | kwGoTo | kwOrElse

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[String, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }

  lazy val exp =
    mergeExp

  lazy val mergeExp =
    positioned(orExp * (monoidMerge ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val monoidMerge =
    positioned(
      kwUnion ^^^ SetMonoid() |
      kwBagUnion ^^^ BagMonoid() |
      kwAppend ^^^ ListMonoid() |
      kwMax ^^^ MaxMonoid())

  lazy val orExp =
    positioned(andExp * (or ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val or =
    positioned(kwOr ^^^ OrMonoid())

  lazy val andExp =
    positioned(comparisonExp * (and ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val and =
    positioned(kwAnd ^^^ AndMonoid())

  lazy val comparisonExp =
    positioned(plusMinusExp * (comparisonOp ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val comparisonOp =
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

  lazy val minus =
    positioned("-" ^^^ Neg())

  lazy val termExp =
    positioned(productExp * (
      sum ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      sub ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val sum =
    positioned("+" ^^^ SumMonoid())

  lazy val sub =
    positioned("-" ^^^ Sub())

  lazy val productExp =
    positioned(baseExp * (
      mult ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      div ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val mult =
    positioned("*" ^^^ MultiplyMonoid())

  lazy val div =
    positioned("/" ^^^ Div())

  /** `baseExp` is left-recursive since `exp` goes down to `baseExp` again.
    */
  lazy val baseExp: PackratParser[Exp] =
    expBlock |
    const |
    recordProj |
    ifThenElse |
    recordConsIdns |
    recordConsIdxs |
    comp |
    zeroAndConsMonoid |
    unaryFun |
    funAbs |
    funApp |
    "(" ~> exp <~ ")" |
    //notExp |
    idnExp

  lazy val expBlock =
    positioned(("{" ~> rep(bind <~ opt(";"))) ~ (exp <~ "}") ^^ { case b ~ e => ExpBlock(b, e) })

  lazy val recordProj =
    positioned(exp ~ ("." ~> attrName) ^^ { case e ~ idn => RecordProj(e, idn) })

  lazy val attrName =
    escapedIdent |
    ident

  lazy val escapedIdent =
    """`[\w\t ]+`""".r ^^ {case s => s.drop(1).dropRight(1) }

  lazy val ident =
    not(reserved) ~> """[_a-zA-Z]\w*""".r

  lazy val const =
    nullConst |
    boolConst |
    stringConst |
    numberConst

  lazy val nullConst =
    positioned("null" ^^^ Null())

  lazy val boolConst =
    positioned(
      kwTrue ^^^ BoolConst(true) |
      kwFalse ^^^ BoolConst(false))

  lazy val stringConst =
    positioned(stringLit ^^ { case s => StringConst(s.drop(1).dropRight(1)) })

  lazy val stringLit =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r

  lazy val numberConst =
    positioned(
      numericLit ^^ { case v =>
        def isInt(s: String) = try { s.toInt; true } catch { case _: Throwable => false }
        if (isInt(v)) IntConst(v) else FloatConst(v)
      })

  lazy val numericLit =
    """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val ifThenElse =
    positioned(kwIf ~> exp ~ (kwThen ~> exp) ~ (kwElse ~> exp) ^^ { case e1 ~ e2 ~ e3 => IfThenElse(e1, e2, e3) })

  lazy val recordConsIdns =
    positioned(
      "(" ~> rep1sep((attrName <~ ":=") ~ exp, ",") <~ ")" ^^ {
        case atts => RecordCons(atts.map{ case idn ~ e => AttrCons(idn, e) })
      })

  lazy val recordConsIdxs =
    positioned(
      "(" ~> (exp <~ ",") ~ rep1sep(exp, ",") <~ ")" ^^ {
        case head ~ tail => RecordCons(tail.+:(head).zipWithIndex.map{ case (e, idx) => AttrCons(s"_${idx + 1}", e) })
      })

  lazy val comp =
    positioned((kwFor ~ "(") ~> (rep1sep(qualifier, ";") <~ ")") ~ (kwYield ~> monoid) ~ exp ^^ { case qs ~ m ~ e => Comp(m, qs, e) })

  lazy val monoid =
    primitiveMonoid |
    collectionMonoid |
    err("illegal monoid")

  lazy val primitiveMonoid =
    positioned(
      kwSum ^^^ SumMonoid() |
      kwMultiply ^^^ MultiplyMonoid() |
      kwMax ^^^ MaxMonoid() |
      kwOr ^^^ OrMonoid() |
      kwAnd ^^^ AndMonoid())

  lazy val collectionMonoid =
    positioned(
      kwSet ^^^ SetMonoid() |
      kwBag ^^^ BagMonoid() |
      kwList ^^^ ListMonoid())

  lazy val qualifier =
    gen |
    bind |
    filter |
    err("illegal qualifier")

  lazy val gen =
    positioned((pattern <~ "<-") ~ exp ^^ { case p ~ e => Gen(p, e) })

  lazy val pattern =
    patternIdn |
    patternProd

  lazy val patternIdn =
    positioned(idnDef ^^ { case idn => PatternIdn(idn) })

  lazy val idnDef =
    positioned(ident ^^ { case idn => IdnDef(idn) })

  lazy val patternProd: PackratParser[Pattern] =
    positioned("(" ~> rep1sep(pattern, ",") <~ ")" ^^ PatternProd)

  lazy val bind =
    positioned((pattern <~ ":=") ~ exp ^^ { case p ~ e => Bind(p, e) })

  lazy val filter =
    exp

  lazy val zeroAndConsMonoid =
    positioned(
      (collectionMonoid <~ "(") ~ (repsep(exp, ",") <~ ")") ^^ {
        case m ~ es  =>
          def clone(m: CollectionMonoid): CollectionMonoid = m match {
            case m: SetMonoid => m.copy()
            case m: BagMonoid => m.copy()
            case m: ListMonoid => m.copy()
          }

          if (es.isEmpty)
            ZeroCollectionMonoid(m)
          else {
            val nes: List[Exp] = es.map(ConsCollectionMonoid(clone(m), _))
            nes.tail.foldLeft(nes.head)((a, b) => MergeMonoid(clone(m), a, b))
          }
        })

  lazy val unaryFun =
    positioned(unaryOp ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => UnaryExp(op, e) })

  lazy val unaryOp =
    positioned(
      kwToBool ^^^ ToBool() |
      kwToInt ^^^ ToInt() |
      kwToFloat ^^^ ToFloat() |
      kwToString ^^^ ToString())

  lazy val funAbs =
    positioned("\\" ~> pattern ~ ("->" ~> exp) ^^ { case p ~ e => FunAbs(p, e) })

  lazy val funApp =
    positioned(exp ~ exp ^^ { case f ~ e => FunApp(f, e) })

  lazy val notExp =
    positioned(not ~ exp ^^ { case op ~ e => UnaryExp(op, e)})

  lazy val not =
    positioned(kwNot ^^^ Not())

  lazy val idnExp =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse =
    positioned(ident ^^ IdnUse)

}