package raw
package calculus

import org.kiama.util.PositionedParserUtilities

/** Parser for monoid comprehensions.
  */
object SyntaxAnalyzer extends PositionedParserUtilities {

  import Calculus._
  import scala.collection.immutable.List
  import scala.collection.immutable.HashSet

  override val whiteSpace =
    """(\s|(//.*\n))+""".r

  val reservedWords = HashSet(
    "or", "and", "not",
    "union", "bag_union", "append", "max", "sum",
    "null", "true", "false",
    "for", "yield",
    "if", "then", "else",
    "bool", "int", "float", "string", "record", "set", "bag", "list",
    "to_bool", "to_int", "to_float", "to_string")

  /** Make an AST by running the parser, reporting errors if the parse fails.
    */
  def apply(query: String): Either[String, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }

  lazy val exp: PackratParser[Exp] =
    mergeExp

  lazy val mergeExp: PackratParser[Exp] =
    positioned(orExp * (monoidMerge ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val monoidMerge: PackratParser[Monoid] =
    positioned(
      kw("union") ^^^ SetMonoid() |
      kw("bag_union") ^^^ BagMonoid() |
      kw("append") ^^^ ListMonoid() |
      kw("max") ^^^ MaxMonoid())

  lazy val orExp: PackratParser[Exp] =
    positioned(andExp * (or ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val or: PackratParser[OrMonoid] =
    positioned(kw("or") ^^^ OrMonoid())

  lazy val andExp: PackratParser[Exp] =
    positioned(comparisonExp * (and ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val and: PackratParser[AndMonoid] =
    positioned(kw("and") ^^^ AndMonoid())

  lazy val notExp: PackratParser[Exp] =
    positioned(not ~ notExp ^^ { case op ~ e => UnaryExp(op, e)}) |
    comparisonExp

  lazy val not: PackratParser[Not] =
    positioned(kw("not") ^^^ Not())

  lazy val comparisonExp: PackratParser[Exp] =
    positioned(plusMinusExp * (comparisonOp ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val comparisonOp: PackratParser[BinaryOperator] =
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
    positioned(baseExp * (
      mult ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      div ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val mult: PackratParser[MultiplyMonoid] =
    positioned("*" ^^^ MultiplyMonoid())

  lazy val div: PackratParser[Div] =
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
   idnExp

  lazy val expBlock: PackratParser[ExpBlock] =
    positioned(("{" ~> rep(bind <~ opt(";"))) ~ (exp <~ "}") ^^ ExpBlock)

  lazy val const: PackratParser[Exp] =
    nullConst |
    boolConst |
    stringConst |
    numberConst

  lazy val nullConst: PackratParser[Null] =
    positioned("null" ^^^ Null())

  lazy val boolConst: PackratParser[BoolConst] =
    positioned(kw("true") ^^^ BoolConst(true)) |
    positioned(kw("false") ^^^ BoolConst(false))

  lazy val stringConst: PackratParser[StringConst] =
    positioned(stringLit ^^ StringConst)

  def stringLit: Parser[String] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r ^^ (s => s.drop(1).dropRight(1))

  lazy val numberConst: PackratParser[NumberConst] =
    positioned(numericLit ^^ { case v =>
      def isInt(s: String) = try { s.toInt; true } catch { case _: Throwable => false }
      if (isInt(v)) IntConst(v) else FloatConst(v)
    })

  def numericLit: Parser[String] =
    """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val recordProj: PackratParser[Exp] =
    positioned(exp ~ ("." ~> attrName) ^^ RecordProj)

  def attrName: Parser[Idn] =
    escapeName |
    ident

  def escapeName: Parser[Idn] =
    """`[\w\t ]+`""".r into (s => success(s.drop(1).dropRight(1)))

  def ident: Parser[Idn] =
    """[_a-zA-Z]\w*""".r into (s => {
      if (reservedWords contains s)
        failure(s"""reserved keyword '${s}' found where identifier expected""")
      else
        success(s)
    })

  lazy val ifThenElse: PackratParser[IfThenElse] =
    positioned(kw("if") ~> exp ~ (kw("then") ~> exp) ~ (kw("else") ~> exp) ^^ IfThenElse)

  lazy val recordConsIdns: PackratParser[RecordCons] =
    positioned("(" ~> rep1sep((attrName <~ ":=") ~ exp, ",") <~ ")" ^^ {
      case atts => RecordCons(atts.map{ case idn ~ e => AttrCons(idn, e) })
    })

  lazy val recordConsIdxs: PackratParser[RecordCons] =
    positioned("(" ~> (exp <~ ",") ~ rep1sep(exp, ",") <~ ")" ^^ {
      case head ~ tail => RecordCons(AttrCons("_1", head) :: tail.zipWithIndex.map{ case (e, idx) => AttrCons(s"_${idx + 2}", e) })
    })

  lazy val comp: PackratParser[Comp] =
    positioned(("for" ~ "(") ~> (rep1sep(qualifier, ";") <~ ")") ~ ("yield" ~> monoid) ~ exp ^^ { case qs ~ m ~ e => Comp(m, qs, e) })

  lazy val monoid: PackratParser[Monoid] =
    primitiveMonoid |
    collectionMonoid |
    failure("illegal monoid")

  lazy val primitiveMonoid: PackratParser[PrimitiveMonoid] =
    positioned(
      kw("sum") ^^^ SumMonoid() |
      kw("multiply") ^^^ MultiplyMonoid() |
      kw("max") ^^^ MaxMonoid() |
      kw("or") ^^^ OrMonoid() |
      kw("and") ^^^ AndMonoid())

  lazy val collectionMonoid: PackratParser[CollectionMonoid] =
    positioned(
      kw("set") ^^^ SetMonoid() |
      kw("bag") ^^^ BagMonoid() |
      kw("list") ^^^ ListMonoid())

  lazy val qualifier: PackratParser[Qual] =
    gen |
    bind |
    filter |
    failure("illegal qualifier")

  lazy val gen: PackratParser[Gen] =
    positioned((pattern <~ "<-") ~ exp ^^ Gen)

  lazy val pattern: PackratParser[Pattern] =
    patternIdn |
    patternProd

  lazy val patternIdn: PackratParser[PatternIdn] =
    positioned(idnDef ^^ PatternIdn)

  lazy val idnDef: PackratParser[IdnDef] =
    positioned(ident ^^ IdnDef)

  lazy val patternProd: PackratParser[PatternProd] =
    positioned("(" ~> rep1sep(pattern, ",") <~ ")" ^^ PatternProd)

  lazy val bind: PackratParser[Bind] =
    positioned((pattern <~ ":=") ~ exp ^^ Bind)

  lazy val filter: PackratParser[Exp] =
    exp

  lazy val zeroAndConsMonoid: PackratParser[Exp] =
    positioned(
    (collectionMonoid <~ "(") ~ (repsep(exp, ",") <~ ")") ^^ {
      case m ~ es  => {
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
      }
    })

  lazy val unaryFun: PackratParser[UnaryExp] =
    positioned(unaryOp ~ ("(" ~> exp <~ ")") ^^ UnaryExp)

  lazy val unaryOp: PackratParser[UnaryOperator] =
    positioned(
      "to_bool" ^^^ ToBool() |
      "to_int" ^^^ ToInt() |
      "to_float" ^^^ ToFloat() |
      "to_string" ^^^ ToString())

  lazy val funAbs: PackratParser[FunAbs] =
    positioned("\\" ~> pattern ~ ("->" ~> exp) ^^ FunAbs)

  lazy val funApp: PackratParser[FunApp] =
    positioned(exp ~ exp ^^ FunApp)

  lazy val idnExp: PackratParser[IdnExp] =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse: PackratParser[IdnUse] =
    positioned(ident ^^ IdnUse)

  def kw(s: String) =
    (s + "\\b").r

}