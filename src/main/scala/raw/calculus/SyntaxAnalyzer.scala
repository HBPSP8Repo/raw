package raw
package calculus

import org.kiama.util.PositionedParserUtilities

/** Parser for monoid comprehensions.
  */
class SyntaxAnalyzer extends PositionedParserUtilities {

  import Calculus._
  import scala.collection.immutable.List
  import scala.collection.immutable.HashSet

  //lexical.delimiters += ("(", ")", "=", "<>", "<=", "<", ">=", ">", "+", "-", "*", "/", ",", ".", ":", ":=", "<-", "->", "\\")

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
  def makeAST(query: String): Either[String, Exp] = parseAll(exp, query) match {
    case Success(ast, _) => Right(ast)
    case f => Left(f.toString)
  }
//  def makeAST(query: String): Either[String, Exp] = phrase(exp)(new lexical.Scanner(query)) match {
//    case Success(ast, _) => Right(ast)
//    case f               => Left(f.toString)
//  }

  lazy val exp: PackratParser[Exp] =
    orExp

  lazy val orExp: PackratParser[Exp] =
    positioned(andExp * (or ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val or: PackratParser[OrMonoid] =
    positioned("or" ^^^ OrMonoid())

  lazy val andExp: PackratParser[Exp] =
    positioned(comparisonExp * (and ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val and: PackratParser[AndMonoid] =
    positioned("and" ^^^ AndMonoid())

  lazy val comparisonExp: PackratParser[Exp] =
    positioned(termExp ~ eqAndCompOp ~ termExp ^^ { case e1 ~ op ~ e2 => BinaryExp(op, e1, e2) }) |
    positioned(not ~ termExp ^^ { case op ~ e => UnaryExp(op, e) }) |
    termExp

  lazy val eqAndCompOp: PackratParser[BinaryOperator] =
    positioned(
      "=" ^^^ Eq() |
      "<>" ^^^ Neq() |
      "<=" ^^^ Le() |
      "<" ^^^ Lt() |
      ">=" ^^^ Ge() |
      ">" ^^^ Gt())

  lazy val not: PackratParser[Not] =
    positioned("not" ^^^ Not())

  lazy val termExp: PackratParser[Exp] =
    positioned(productExp * (
      sum ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      sub ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val sum: PackratParser[SumMonoid] =
    positioned("+" ^^^ SumMonoid())

  lazy val sub: PackratParser[Sub] =
    positioned("-" ^^^ Sub())

  lazy val productExp: PackratParser[Exp] =
    positioned(compExp * (
      mult ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } } |
      div ^^ { case op => { (e1: Exp, e2: Exp) => BinaryExp(op, e1, e2) } }))

  lazy val mult: PackratParser[MultiplyMonoid] =
    positioned("*" ^^^ MultiplyMonoid())

  lazy val div: PackratParser[Div] =
    positioned("/" ^^^ Div())

  lazy val compExp: PackratParser[Exp] =
    comp |
    mergeExp

  lazy val mergeExp: PackratParser[Exp] =
    positioned(baseExp * (monoidMerge ^^ { case op => { (e1: Exp, e2: Exp) => MergeMonoid(op, e1, e2) } }))

  lazy val monoidMerge: PackratParser[Monoid] =
    positioned(
      "union" ^^^ SetMonoid() |
      "bag_union" ^^^ BagMonoid() |
      "append" ^^^ ListMonoid() |
      "max" ^^^ MaxMonoid())

  /** `baseExp` is left-recursive since `exp` goes down to `baseExp` again.
    */
  lazy val baseExp: PackratParser[Exp] =
    const |
    recordProj |
    productProj |
    ifThenElse |
    productCons |
    recordCons |
    zeroAndConsMonoid |
    unaryFun |
    funAbs |
    funApp |
    "(" ~> exp <~ ")" |
    idnExp

  lazy val const: PackratParser[Exp] =
    nullConst |
    boolConst |
    stringConst |
    numberConst |
    positioned(neg ~ numberConst ^^ { case op ~ e => UnaryExp(op, e) })

  lazy val nullConst: PackratParser[Null] =
    positioned("null" ^^^ Null())

  lazy val boolConst: PackratParser[BoolConst] =
    positioned("true" ^^^ BoolConst(true)) |
    positioned("false" ^^^ BoolConst(false))

  lazy val stringConst: PackratParser[StringConst] =
    positioned(stringLit ^^ StringConst)

  def stringLit: Parser[String] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r ^^ (s => s.drop(1).dropRight(1))

  lazy val neg: PackratParser[Neg] =
    positioned("-" ^^^ Neg())

  lazy val numberConst: PackratParser[NumberConst] =
    positioned(numericLit ^^ { case v =>
      def isInt(s: String) = try { s.toInt; true } catch { case _: Throwable => false }
      if (isInt(v)) IntConst(v) else FloatConst(v)
      })

  def numericLit: Parser[String] =
    """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val recordProj: PackratParser[Exp] =
    positioned(exp ~ ("." ~> ident) ^^ RecordProj)

  def ident: Parser[Idn] =
    escapeName |
      varName

  def escapeName: Parser[Idn] =
    """`[\w\t ]+`""".r into (s => success(s.drop(1).dropRight(1)))

  def varName: Parser[Idn] =
    """[a-zA-Z]\w*""".r into (s => {
      if (reservedWords contains s)
        failure(s"""reserved keyword '${s}' found where identifier expected""")
      else
        success(s)
    })

  lazy val productProj: PackratParser[Exp] =
    positioned(exp ~ ("." ~> integer) ^^ ProductProj)

  def integer: Parser[Int] =
    """\d+""".r into (s => success(s.toInt))

  lazy val ifThenElse: PackratParser[IfThenElse] =
    positioned("if" ~> exp ~ ("then" ~> exp) ~ ("else" ~> exp) ^^ { case e1 ~ e2 ~ e3 => IfThenElse(e1, e2, e3) })

  lazy val productCons: PackratParser[ProductCons] =
    positioned(("(" ~> exp) ~ ("," ~> rep1sep(exp, ",") <~ ")") ^^ {
      case head ~ tail => ProductCons(head :: tail)
    })

  lazy val recordCons: PackratParser[RecordCons] =
    positioned("(" ~> repsep(attrCons, ",") <~ ")" ^^ RecordCons)

  lazy val attrCons: PackratParser[AttrCons] =
    positioned((ident <~ ":=") ~ exp ^^ { case idn ~ e => AttrCons(idn, e) })

  lazy val comp: PackratParser[Comp] =
    positioned(("for" ~ "(") ~> (repsep(qualifier, ",") <~ ")") ~ ("yield" ~> monoid) ~ exp ^^ { case qs ~ m ~ e => Comp(m, qs, e) })

  lazy val monoid: PackratParser[Monoid] =
    primitiveMonoid |
    collectionMonoid |
    failure("illegal monoid")

  lazy val primitiveMonoid: PackratParser[PrimitiveMonoid] =
    positioned(
      "sum" ^^^ SumMonoid() |
      "multiply" ^^^ MultiplyMonoid() |
      "max" ^^^ MaxMonoid() |
      "or" ^^^ OrMonoid() |
      "and" ^^^ AndMonoid())

  lazy val collectionMonoid: PackratParser[CollectionMonoid] =
    positioned(
      "set" ^^^ SetMonoid() |
      "bag" ^^^ BagMonoid() |
      "list" ^^^ ListMonoid())

  lazy val qualifier: PackratParser[Qual] =
    gen |
    bind |
    filter |
    failure("illegal qualifier")

  lazy val gen: PackratParser[Gen] =
    positioned((idnDef <~ "<-") ~ exp ^^ { case idn ~ e => Gen(idn, e) })

  lazy val idnDef: PackratParser[IdnDef] =
  // TODO: ???
    positioned(ident ~ opt(":" ~> tipe) ^^ IdnDef)

  lazy val bind: PackratParser[Bind] =
    positioned((idnDef <~ ":=") ~ exp ^^ { case idn ~ e => Bind(idn, e) })

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
    positioned(unaryOp ~ ("(" ~> exp <~ ")") ^^ { case op ~ e => UnaryExp(op, e) })

  lazy val unaryOp: PackratParser[UnaryOperator] =
    positioned(
      "to_bool" ^^^ ToBool() |
      "to_int" ^^^ ToInt() |
      "to_float" ^^^ ToFloat() |
      "to_string" ^^^ ToString())

  lazy val funAbs: PackratParser[FunAbs] =
    positioned("\\" ~> idnDef ~ ("->" ~> exp) ^^ FunAbs)

  lazy val tipe: PackratParser[Type] =
    primitiveType |
    recordType |
    collectionType |
    classType |
    failure("illegal type")

  lazy val primitiveType: PackratParser[PrimitiveType] =
    positioned(
      "bool" ^^^ BoolType() |
      "int" ^^^ IntType() |
      "float" ^^^ FloatType() |
      "string" ^^^ StringType())

  lazy val recordType: PackratParser[RecordType] =
    positioned("record" ~ "(" ~> repsep(attr, ",") <~ ")" ^^ RecordType)

  lazy val attr: PackratParser[AttrType] =
    positioned((ident <~ ":") ~ tipe ^^ { case idn ~ t => AttrType(idn, t) })

  lazy val collectionType: PackratParser[CollectionType] =
    positioned(
      ("bag" ~ "(") ~> (tipe <~ ")") ^^ { case t => BagType(t) } |
      ("list" ~ "(") ~> (tipe <~ ")") ^^ { case t => ListType(t) } |
      ("set" ~ "(") ~> (tipe <~ ")") ^^ { case t => SetType(t) }
    )

  lazy val classType: PackratParser[ClassType] =
    positioned(ident ^^ ClassType)

  lazy val funApp: PackratParser[FunApp] =
    positioned(exp ~ ("(" ~> exp <~ ")") ^^ { case e1 ~ e2 => FunApp(e1, e2) })

  lazy val idnExp: PackratParser[IdnExp] =
    positioned(idnUse ^^ IdnExp)

  lazy val idnUse: PackratParser[IdnUse] =
  // TODO: ???
    positioned(ident ^^ IdnUse)

}
