/** Parser for monoid comprehensions.
 */
package raw.calculus

import org.kiama.util.PositionedParserUtilities
import raw._

class SyntaxAnalyzer extends PositionedParserUtilities {

  import scala.collection.immutable.HashSet
  import org.kiama.attribution.Attribution.initTree
  import Calculus._

  val reservedWords = HashSet("or",
    "and",
    "not",
    "union", "bag_union", "append", "max",
    "null", "true", "false",
    "for", "yield",
    "bool", "int", "float", "string", "record", "set", "bag", "list",
    "to_bool", "to_int", "to_float", "to_string")

  /** Make an AST by running the parser, reporting errors if the parse fails.
   */
  def makeAST(query: String): Either[String, Comp] = parseAll(parser, query) match {
    case Success(ast, _) => initTree(ast); Right(ast)
    case f               => Left(f.toString)
  }

  def parser: Parser[Comp] = phrase(comp)

  def exp: Parser[Exp] = orExp

  // TODO: Add support for remaining monoids - max, union, bag_union, append - as MergeMonoid expressions
  // TODO: Sort out the relative priorities between MergeMonoids + BinaryExp

  def orExp: Parser[Exp] =
    andExp * (or ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } })

  def or: Parser[OrMonoid] =
    "or" ^^^ OrMonoid()

  def andExp: Parser[Exp] =
    comparisonExp * (and ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } })

  def and: Parser[AndMonoid] =
    "and" ^^^ AndMonoid()

  def comparisonExp: Parser[Exp] =
    termExp ~ eqAndCompOp ~ termExp ^^ { case e1 ~ op ~ e2 => BinaryExp(op, e1, e2) } |
    not ~ termExp ^^ UnaryExp |
    termExp

  def eqAndCompOp: Parser[BinaryOperator] =
    "=" ^^^ Eq() |
    "<>" ^^^ Neq() |
    "<=" ^^^ Le() |
    "<" ^^^ Lt() |
    ">=" ^^^ Ge() |
    ">" ^^^ Gt()

  def not: Parser[Not] =
    "not" ^^^ Not()

  def termExp: Parser[Exp] =
    productExp *
      (sum ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
       sub ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } })

  def sum: Parser[SumMonoid] =
    "+" ^^^ SumMonoid()

  def sub: Parser[Sub] =
    "-" ^^^ Sub()

  def productExp: Parser[Exp] =
    mergeExp *
      (mult ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
       div ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } })

  def mult: Parser[MultiplyMonoid] =
    "*" ^^^ MultiplyMonoid()

  def div: Parser[Div] =
    "/" ^^^ Div()

  def mergeExp: Parser[Exp] =
    baseExp * (monoidMerge ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } })

  def monoidMerge: Parser[Monoid] =
    "union" ^^^ SetMonoid() |
    "bag_union" ^^^ BagMonoid() |
    "append" ^^^ ListMonoid() |
    "max" ^^^ MaxMonoid()

  /** `baseExp` is a PackratParser since the grammar is left-recursive.
   */
  lazy val baseExp: PackratParser[Exp] =
    const |
    ifThenElse |
    recordCons |
    comp |
    zeroAndConsMonoid |
    unaryFun |
    funAbs |      
    funApp |
    recordProj |      
    "(" ~> exp <~ ")" |
    idnExp

  def const: Parser[Exp] =
    nullConst |
    boolConst |
    stringConst |
    opt(neg) ~ (floatConst | intConst) ^^ { case neg ~ n => neg match {
      case Some(neg) => UnaryExp(neg, n)
      case None => n
      }
    }

  // TODO: Should `neg` be at the baseExp level, under `unaryFun` ???

  def nullConst: Parser[Null] =
    "null" ^^^ Null()

  def boolConst: Parser[BoolConst] =
    "true" ^^^ BoolConst(true) |
    "false" ^^^ BoolConst(false)

  def stringConst: Parser[StringConst] =
    stringLit ^^ StringConst

  def stringLit: Parser[String] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r ^^ (s => s.drop(1).dropRight(1))

  def neg: Parser[Neg] =
    "-" ^^^ Neg()

  def intConst: Parser[IntConst] =
    int ^^ IntConst

  def int: Parser[Integer] =
    """\d+""".r ^^ { case v => new Integer(v.toInt) }

  def floatConst: Parser[FloatConst] =
    floatingPointNumber ^^ FloatConst

  /** The alternatives below ensure that the floating point parser NEVER succeeds if the input is a simple integer.
   */
  def floatingPointNumber: Parser[Float] =
    """\d+\.\d*([eE][+-]?\d+)?""".r ^^ (s => s.toFloat) |
    """\.\d+([eE][+-]?\d+)?""".r ^^ (s => s.toFloat) |
    """\d+[eE][+-]?\d+""".r ^^ (s => s.toFloat)

  def ifThenElse: Parser[IfThenElse] =
    "if" ~> exp ~ ("then" ~> exp) ~ ("else" ~> exp) ^^ IfThenElse

  def recordCons: Parser[RecordCons] =
    "(" ~> repsep(attrCons, ",") <~ ")" ^^ RecordCons

  def attrCons: Parser[AttrCons] =
    (ident <~ ":=") ~ exp ^^ AttrCons

  def ident: Parser[Idn] =
    """[a-zA-Z]\w*""".r into (s => {
      if (reservedWords contains s)
        failure(s"""reserved keyword '${s}' found where identifier expected""")
      else
        success(s)
    })

  def comp: Parser[Comp] =
    ("for" ~ "(") ~> (repsep(qualifier, ",") <~ ")") ~ ("yield" ~> monoid) ~ exp ^^ { case qs ~ m ~ e => Comp(m, qs, e) }

  def monoid: Parser[Monoid] =
    primitiveMonoid |
    collectionMonoid |
    failure("illegal monoid")

  def primitiveMonoid: Parser[PrimitiveMonoid] =
    "sum" ^^^ SumMonoid() |
    "multiply" ^^^ MultiplyMonoid() |
    "max" ^^^ MaxMonoid() |
    "or" ^^^ OrMonoid() |
    "and" ^^^ AndMonoid()

  def collectionMonoid: Parser[CollectionMonoid] =
    "set" ^^^ SetMonoid() |
    "bag" ^^^ BagMonoid() |
    "list" ^^^ ListMonoid()

  def qualifier: Parser[Qual] =
    gen |
    bind |
    filter |
    failure("illegal qualifier")

  def gen: Parser[Gen] =
    (idnDef <~ "<-") ~ exp ^^ Gen

  def idnDef: Parser[IdnDef] =
    ident ^^ IdnDef

  def bind: Parser[Bind] =
    (idnDef <~ ":=") ~ exp ^^ Bind

  def filter: Parser[Exp] =
    exp

  def zeroAndConsMonoid: Parser[Exp] =
    setZeroAndConsMonoid |
      bagZeroAndConsMonoid |
      listZeroAndConsMonoid

  def setZeroAndConsMonoid: Parser[Exp] =
    openSet ~ opt(exp) <~ closeSet ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(m, e)
        case None    => ZeroCollectionMonoid(m)
      }
    }

  def openSet: Parser[SetMonoid] =
    "{" ^^^ SetMonoid()

  def closeSet: Parser[String] =
    "}"

  def bagZeroAndConsMonoid: Parser[Exp] =
    openBag ~ opt(exp) <~ closeBag ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(m, e)
        case None    => ZeroCollectionMonoid(m)
      }
    }

  def openBag: Parser[BagMonoid] =
    "bag{" ^^^ BagMonoid()

  def closeBag: Parser[String] =
    "}"

  def listZeroAndConsMonoid: Parser[Exp] =
    openList ~ opt(exp) <~ closeList ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(m, e)
        case None    => ZeroCollectionMonoid(m)
      }
    }

  def openList: Parser[ListMonoid] =
    "[" ^^^ ListMonoid()

  def closeList: Parser[String] =
    "]"

  def unaryFun: Parser[UnaryExp] =
    unaryOp ~ ("(" ~> exp <~ ")") ^^ UnaryExp

  def unaryOp: Parser[UnaryOperator] =
    "to_bool" ^^^ ToBool() |
    "to_float" ^^^ ToFloat() |
    "to_int" ^^^ ToInt() |
    "to_string" ^^^ ToString()

  def funAbs: Parser[FunAbs] =
    "\\" ~> idnDef ~ (":" ~> tipe) ~ ("=>" ~> exp) ^^ FunAbs

  def tipe: Parser[Type] =
    primitiveType |
    recordType |
    collectionType |
    classType |
    failure("illegal type")

  def primitiveType: Parser[PrimitiveType] =
    "bool" ^^^ BoolType() |
    "float" ^^^ FloatType() |
    "int" ^^^ IntType() |
    "string" ^^^ StringType()

  def recordType: Parser[RecordType] =
    "record" ~ "(" ~> repsep(attr, ",") <~ ")" ^^ RecordType

  def attr: Parser[AttrType] =
    (ident <~ ":") ~ tipe ^^ AttrType

  def collectionType: Parser[CollectionType] =
    collectionTypeMonoid ~ ("(" ~> tipe <~ ")") ^^ CollectionType

  def collectionTypeMonoid: Parser[CollectionMonoid] =
    "bag" ^^^ BagMonoid() |
    "list" ^^^ ListMonoid() |
    "set" ^^^ SetMonoid()

  def classType: Parser[ClassType] =
    ident ^^ ClassType

  def funApp: Parser[FunApp] =
    exp ~ ("(" ~> exp <~ ")") ^^ FunApp

  def recordProj: Parser[Exp] =
    exp ~ rep1("." ~> ident) ^^ { case e ~ projs => { projs.foldLeft(e)((projectExp, id) => RecordProj(projectExp, id)) } }

  def idnExp: Parser[IdnExp] =
    idnUse ^^ IdnExp

  def idnUse: Parser[IdnUse] =
    ident ^^ IdnUse

}