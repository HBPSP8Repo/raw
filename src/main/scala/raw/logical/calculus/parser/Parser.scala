/** Parser for monoid comprehensions with built-in type checker.
 *  The type checker rules are described in [1] (Fig. 3, page 13).
 */
package raw.logical.calculus.parser

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.Position
import scala.util.parsing.input.Positional

import raw._
import raw.logical.Catalog

/**
 * ParserError
 */
case class ParserError(line: Int, column: Int, err: String) extends RawException(err)

/**
 * TypeCheckerError
 */
sealed abstract class TypeCheckerError(val err: String) extends RawException(err)

case class PrimitiveTypeRequired(e: TypedExpression) extends TypeCheckerError("primitive type required")
case class BoolRequired(e: TypedExpression) extends TypeCheckerError("bool required")
case class NumberRequired(e: TypedExpression) extends TypeCheckerError("number required")
case class MonoidMergeMismatch(m: Monoid, e1: TypedExpression, e2: TypedExpression) extends TypeCheckerError("monoid merge mismatch")
case class UnknownAttribute(name: String, pos: Position) extends TypeCheckerError("unknown attribute '" + name + "'")
case class RecordRequired(e: TypedExpression) extends TypeCheckerError("record required")
case class IfResultMismatch(e1: TypedExpression, e2: TypedExpression) extends TypeCheckerError("if/then/else mismatch")
case class FunctionApplicationMismatch(e1: TypedExpression, e2: TypedExpression) extends TypeCheckerError("function application mismatch")
case class FunctionTypeRequired(e: TypedExpression) extends TypeCheckerError("function type required")
case class CommutativeMonoidRequired(m: Monoid, e: TypedExpression) extends TypeCheckerError("commutative monoid required")
case class IdempotentMonoidRequired(m: Monoid, e: TypedExpression) extends TypeCheckerError("idempotent monoid required")
case class PredicateRequired(e: TypedExpression) extends TypeCheckerError("predicate required")
case class CollectionTypeRequired(e: TypedExpression) extends TypeCheckerError("collection type required")
case class ConversionMismatch(from: ParserType, to: ParserType) extends RawException("conversion mismatch from " + from + " to " + to)

/**
 * Parser
 */
class Parser(val catalog: Catalog) extends StandardTokenParsers {

  private def buildScope() = {
    def convertType(t: MonoidType): ParserType = t match {
      case raw.BoolType => BoolType
      case raw.StringType => StringType
      case raw.FloatType => FloatType
      case raw.IntType => IntType
      case raw.RecordType(atts) => RecordType(atts.map(att => Attribute(att.name, convertType(att.monoidType))))
      case raw.SetType(t) => SetType(convertType(t))
      case raw.BagType(t) => BagType(convertType(t))
      case raw.ListType(t) => ListType(convertType(t))
      case raw.VariableType => throw RawInternalException("variable type not allowed for catalog types")
    }

    val rootScope = new RootScope()
    for ((name, monoidType) <- catalog.catalog)
      rootScope.bind(name, Variable(convertType(monoidType), name))
    rootScope
  }

  private val rootScope = buildScope()

  lexical.reserved ++= Set(
    "not",
    "union", "bag_union", "append", "sum", "multiply", "max", "or", "and",
    "null", "true", "false",
    "if", "else", "then",
    "bool", "int", "float", "string", "record", "set", "bag", "list",
    "for", "yield")

  lexical.delimiters ++= Set(
    "=", "<>", "<=", "<", ">=", ">",
    "+", "-", "*", "/",
    ".", "(", ")", "[", "]", "{", "}", ",", "\\", ":", "=>", "<-", "`", ":=")

  /**
   * CurrentScope
   *
   *  Holds the current (active) scope and provides wrapper methods to manipulate scopes.
   */
  private class CurrentScope(var scope: Scope) {
    def bind(name: String, v: Variable) = scope.bind(name, v)

    def add() = scope = scope.add()

    def exists(name: String) = scope.exists(name)

    def del() = scope match {
      case RootScope() => throw RawInternalException("parser requested deletion of top scope")
      case InnerScope(parent) => scope = parent
    }

    def get(name: String) = scope.get(name)
  }

  private val scope = new CurrentScope(rootScope)

  /**
   * Type Unification Algorithm
   *
   * The following type unification algorithm is described in:
   *   http://inst.eecs.berkeley.edu/~cs164/sp11/lectures/lecture22.pdf (Slide 9)
   */
  def unify(t1: ParserType, t2: ParserType): Option[ParserType] = {
    def recurse(t1: ParserType, t2: ParserType, binding: Map[ParserType, ParserType]): Option[Map[ParserType, ParserType]] = {
      val T1 = binding.getOrElse(t1, t1)
      val T2 = binding.getOrElse(t2, t2)
      if (T1 == T2) {
        Some(binding + (T1 -> T1, T2 -> T2))
      } else {
        (T1, T2) match {
          case (IntType, FloatType) => Some(binding + (T1 -> FloatType, T2 -> FloatType))
          case (FloatType, IntType) => Some(binding + (T1 -> FloatType, T2 -> FloatType))
          case _ => {
            T1 match {
              case VariableType() => Some(binding + (T1 -> T2))
              case _ => {
                val nbinding = binding + (T2 -> T1)
                T2 match {
                  case VariableType() => Some(nbinding)
                  case _ => {
                    (T1, T2) match {
                      case (RecordType(atts1), RecordType(atts2)) => {
                        (atts1, atts2).zipped map ((att1, att2) =>
                          recurse(att1.parserType, att2.parserType, nbinding)) reduce (
                          (_, _) match {
                            case (Some(a), Some(b)) => Some(a ++ b)
                            case _ => None
                          })
                      }
                      case (c1: CollectionType, c2: CollectionType) => recurse(c1.innerType, c2.innerType, nbinding)
                      case _ => None
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    recurse(t1, t2, Map()) match {
      case Some(u) => Some(u.getOrElse(t1, u(t2)))
      case _ => None
    }
  }

  /**
   * Type conversion nodes.
   */
  def cast(t: ParserType, e: TypedExpression) = (e.parserType, t) match {
    case (BoolType, BoolType) => e
    case (IntType, IntType) => e
    case (IntType, FloatType) => IntToFloat(e)
    case (IntType, StringType) => IntToString(e)
    case (FloatType, FloatType) => e
    case (FloatType, IntType) => FloatToInt(e)
    case (FloatType, StringType) => FloatToString(e)
    case (StringType, StringType) => e
    case (StringType, FloatType) => StringToFloat(e)
    case (StringType, IntType) => StringToInt(e)
    case (StringType, BoolType) => StringToBool(e)
    case (SetType(t1), SetType(t2)) if t1 == t2 => e
    case (BagType(t1), BagType(t2)) if t1 == t2 => e
    case (ListType(t1), ListType(t2)) if t1 == t2 => e
    case _ => throw ConversionMismatch(e.parserType, t)
  }

  def expression: Parser[TypedExpression] = positioned(cmpExpr)

  def cmpExpr: Parser[TypedExpression] = positioned(
    addExpr ~ rep(comparison ~ addExpr ^^ { case op ~ rhs => (op, rhs) }) ^^ {
      case lhs ~ elems => {
        elems.foldLeft(lhs) {
          case (acc, ((op, rhs: TypedExpression))) => {
            unify(acc.parserType, rhs.parserType) match {
              case Some(ut: PrimitiveType) => BinaryOperation(BoolType, op, cast(ut, acc), cast(ut, rhs))
              case _ => {
                acc.parserType match {
                  case t: PrimitiveType => throw PrimitiveTypeRequired(rhs)
                  case _ => throw PrimitiveTypeRequired(acc)
                }
              }
            }
          }
        }
      }
    } |
      "not" ~> cmpExpr ^^ {
        case e if e.parserType == BoolType => Not(e)
        case e => throw BoolRequired(e)
      })

  def comparison: Parser[ComparisonOperator] = positioned(
    "=" ^^^ Eq() |
      "<>" ^^^ Neq() |
      "<=" ^^^ Le() |
      "<" ^^^ Lt() |
      ">=" ^^^ Ge() |
      ">" ^^^ Gt())

  def addExpr: Parser[TypedExpression] = positioned(
    multExpr * (
      addSub ^^ {
        case op => {
          (e1: TypedExpression, e2: TypedExpression) =>
            {
              unify(e1.parserType, e2.parserType) match {
                case Some(ut: NumberType) => BinaryOperation(ut, op, cast(ut, e1), cast(ut, e1))
                case _ => {
                  e1.parserType match {
                    case t: NumberType => throw NumberRequired(e2)
                    case _ => throw NumberRequired(e1)
                  }
                }
              }
            }
        }
      }))

  def addSub: Parser[ArithmeticOperator] = positioned(
    "+" ^^^ Add() |
      "-" ^^^ Sub())

  def multExpr: Parser[TypedExpression] = positioned(
    mergeExpr * (
      multDiv ^^ {
        case op => {
          (e1: TypedExpression, e2: TypedExpression) =>
            {
              unify(e1.parserType, e2.parserType) match {
                case Some(ut: NumberType) => BinaryOperation(ut, op, cast(ut, e1), cast(ut, e1))
                case _ => {
                  e1.parserType match {
                    case t: NumberType => throw NumberRequired(e2)
                    case _ => throw NumberRequired(e1)
                  }
                }
              }
            }
        }
      }))

  def multDiv: Parser[ArithmeticOperator] = positioned(
    "*" ^^^ Mult() |
      "/" ^^^ Div())

  def mergeExpr: Parser[TypedExpression] = positioned(
    recordProjExpr * (
      monoidMerge ^^ {
        case m => {
          (e1: TypedExpression, e2: TypedExpression) =>
            {
              (unify(e1.parserType, e2.parserType), m) match {
                // Allow strings to be "summed" together even though they are not NumberMonoid
                case (Some(t @ StringType), m: SumMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ IntType), m: NumberMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ FloatType), m: NumberMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ BoolType), m: BoolMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ SetType(_)), m: SetMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ BagType(_)), m: BagMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case (Some(t @ ListType(_)), m: ListMonoid) => MergeMonoid(t, m, cast(t, e1), cast(t, e2))
                case _ => throw MonoidMergeMismatch(m, e1, e2)
              }
            }
        }
      }))

  def monoidMerge: Parser[Monoid] = positioned(
    "union" ^^^ SetMonoid() |
      "bag_union" ^^^ BagMonoid() |
      "append" ^^^ ListMonoid() |
      "sum" ^^^ SumMonoid() |
      "multiply" ^^^ MultiplyMonoid() |
      "max" ^^^ MaxMonoid() |
      "or" ^^^ OrMonoid() |
      "and" ^^^ AndMonoid())

  def recordProjExpr: Parser[TypedExpression] = positioned(
    funcAppExpr ~ rep("." ~> posIdent) ^^ {
      case e ~ projs => {
        def recurse(e: TypedExpression, names: List[PositionedIdent]): TypedExpression = {
          if (names.isEmpty)
            e
          else {
            e.parserType match {
              case RecordType(atts) => {
                val head = names.head
                val name = head.s
                atts.find(_.name == name) match {
                  case Some(att) => recurse(RecordProjection(att.parserType, e, name), names.tail)
                  case _ => throw UnknownAttribute(name, head.pos)
                }
              }
              case _ => throw RecordRequired(e)
            }
          }
        }
        recurse(e, projs)
      }
    })

  /**
   * PositionedIdent and posIdent are used to return identifiers wrapped in Positional.
   */
  case class PositionedIdent(s: String) extends Positional
  def posIdent: Parser[PositionedIdent] = positioned(ident ^^ (PositionedIdent(_)))

  def funcAppExpr: Parser[TypedExpression] = positioned(
    basicExpr ~ opt("(" ~> expression <~ ")") ^^ {
      case f ~ None => f
      case f ~ Some(e) => {
        f.parserType match {
          case FunctionType(t1, t2) => {
            if (t1 == e.parserType)
              FunctionApplication(t2, f, e)
            else
              throw FunctionApplicationMismatch(f, e)
          }
          case _ => throw FunctionTypeRequired(f)
        }
      }
    })

  def basicExpr: Parser[TypedExpression] = positioned(
    constant |
      zeroAndMonoidCons |
      recordCons |
      ifThenElse |
      explicitConversion |
      funcAbs |
      comprehension |
      "(" ~> expression <~ ")" |
      variable)

  def constant: Parser[TypedExpression] = positioned(
    "null" ^^^ Null() |
      "true" ^^^ BoolConst(true) |
      "false" ^^^ BoolConst(false) |
      (numericLit <~ ".") ~ numericLit ^^ { case v1 ~ v2 => FloatConst((v1 + "." + v2).toFloat) } |
      numericLit ^^ { case v => IntConst(v.toInt) } |
      stringLit ^^ { case v => StringConst(v) })

  def zeroAndMonoidCons: Parser[TypedExpression] = positioned(
    setZeroAndMonoidCons |
      bagZeroAndMonoidCons |
      listZeroAndMonoidCons)

  def setZeroAndMonoidCons: Parser[TypedExpression] = positioned(
    setMonoid ~ opt(expression) <~ "}" ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(SetType(e.parserType), m, e)
        case None => EmptySet()
      }
    })

  def setMonoid: Parser[SetMonoid] = positioned(
    "{" ^^^ SetMonoid())

  def bagZeroAndMonoidCons: Parser[TypedExpression] = positioned(
    bagMonoid ~ opt(expression) <~ "}" ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(BagType(e.parserType), m, e)
        case None => EmptyBag()
      }
    })

  def bagMonoid: Parser[BagMonoid] = positioned(
    "bag" ~ "{" ^^^ BagMonoid())

  def listZeroAndMonoidCons: Parser[TypedExpression] = positioned(
    listMonoid ~ opt(expression) <~ "]" ^^ {
      case m ~ e => e match {
        case Some(e) => ConsCollectionMonoid(ListType(e.parserType), m, e)
        case None => EmptyList()
      }
    })

  def listMonoid: Parser[ListMonoid] = positioned(
    "[" ^^^ ListMonoid())

  def recordCons: Parser[TypedExpression] = positioned(
    "(" ~> repsep(attrCons, ",") <~ ")" ^^ {
      case attcons => RecordConstruction(RecordType(attcons.map(att => Attribute(att.name, att.e.parserType))), attcons)
    })

  def attrCons: Parser[AttributeConstruction] = positioned(
    (ident <~ ":=") ~ expression ^^ {
      case name ~ e => AttributeConstruction(name, e)
    })

  def ifThenElse: Parser[TypedExpression] = positioned(
    "if" ~> expression ~ ("then" ~> expression) ~ ("else" ~> expression) ^^ {
      case e1 ~ e2 ~ e3 => {
        e1.parserType match {
          case BoolType => {
            unify(e2.parserType, e3.parserType) match {
              case Some(ut) => IfThenElse(ut, e1, cast(ut, e2), cast(ut, e3))
              case _ => throw IfResultMismatch(e2, e3)
            }
          }
          case _ => throw BoolRequired(e1)
        }
      }
    })

  def explicitConversion: Parser[TypedExpression] = positioned(
    "as_bool" ~ "(" ~> expression <~ ")" ^^ (cast(BoolType, _)) |
      "as_float" ~ "(" ~> expression <~ ")" ^^ (cast(FloatType, _)) |
      "as_int" ~ "(" ~> expression <~ ")" ^^ (cast(IntType, _)) |
      "as_string" ~ "(" ~> expression <~ ")" ^^ (cast(StringType, _)))

  def funcAbs: Parser[TypedExpression] = positioned(
    ("\\" ~> ident ~ (":" ~> parserType) ^^ {
      case name ~ t => {
        val v = Variable(t, name)
        scope.add()
        scope.bind(name, v)
        v
      }
    }) ~ ("=>" ~> expression) ^^ {
      case v ~ e => {
        scope.del()
        FunctionAbstraction(FunctionType(v.parserType, e.parserType), v, e)
      }
    })

  def parserType: Parser[ParserType] =
    "bool" ^^^ BoolType |
      "int" ^^^ IntType |
      "float" ^^^ FloatType |
      "string" ^^^ StringType |
      "record" ~ "(" ~> repsep(attribute, ",") <~ ")" ^^ (RecordType(_)) |
      "set" ~ "(" ~> parserType <~ ")" ^^ (SetType(_)) |
      "bag" ~ "(" ~> parserType <~ ")" ^^ (BagType(_)) |
      "list" ~ "(" ~> parserType <~ ")" ^^ (ListType(_)) |
      failure("illegal type")

  def attribute: Parser[Attribute] =
    (ident <~ ":") ~ parserType ^^ { case name ~ t => Attribute(name, t) }

  def comprehension: Parser[TypedExpression] = positioned(
    (("for" ^^^ scope.add()) <~ "(") ~> (repsep(qualifier, ",") <~ ")") ~ ("yield" ~> monoid) ~ expression ^^ {
      case qs ~ m ~ e => {
        qs.map {
          case Generator(v, g) => {
            g.parserType match {
              case SetType(t) => if (!m.commutative) throw CommutativeMonoidRequired(m, g) else if (!m.idempotent) throw IdempotentMonoidRequired(m, g)
              case BagType(t) => if (!m.commutative) throw CommutativeMonoidRequired(m, g)
              case ListType(t) =>
              // TODO: Review warning on non-exhaustive match
            }
          }
          case q: Bind =>
          case q: TypedExpression => if (q.parserType != BoolType) throw PredicateRequired(q)
        }
        scope.del()
        m match {
          case m: PrimitiveMonoid => Comprehension(e.parserType, m, e, qs)
          case m: SetMonoid => Comprehension(SetType(e.parserType), m, e, qs)
          case m: BagMonoid => Comprehension(BagType(e.parserType), m, e, qs)
          case m: ListMonoid => Comprehension(ListType(e.parserType), m, e, qs)
        }
      }
    })

  def monoid: Parser[Monoid] = positioned(
    "set" ^^^ SetMonoid() |
      "bag" ^^^ BagMonoid() |
      "list" ^^^ ListMonoid() |
      "sum" ^^^ SumMonoid() |
      "multiply" ^^^ MultiplyMonoid() |
      "max" ^^^ MaxMonoid() |
      "or" ^^^ OrMonoid() |
      "and" ^^^ AndMonoid() |
      failure("illegal monoid"))

  def qualifier: Parser[Expression] = positioned(
    generator |
      bind |
      filter |
      failure("illegal qualifier"))

  def generator: Parser[UntypedExpression] = positioned(
    (ident <~ "<-") ~ expression ^^ {
      case name ~ e => {
        e.parserType match {
          case t: CollectionType => {
            val v = Variable(t.innerType, name)
            scope.bind(name, v)
            Generator(v, e)
          }
          case _ => throw CollectionTypeRequired(e)
        }
      }
    })

  def bind: Parser[UntypedExpression] = positioned(
    (ident <~ ":=") ~ expression ^^ {
      case name ~ e => {
        val v = Variable(e.parserType, name)
        scope.bind(name, v)
        Bind(v, e)
      }
    })

  def filter: Parser[TypedExpression] = positioned(
    expression)

  def variable: Parser[Variable] = positioned(
    ident ^? (
      {
        case name if scope.exists(name) =>
          scope.get(name) match {
            case Some(v) => v
            case _ => throw RawInternalException("parser cannot find variable ought to exist")
          }
      },
      name => "variable '" + name + "' does not exist"))

  /*
   * Parse the query and normalize its result.
   */
  def parse(query: String): TypedExpression =
    phrase(expression)(new lexical.Scanner(query)) match {
      case Success(result, next) => Normalizer(result)
      case NoSuccess(msg, next) => throw ParserError(next.pos.line, next.pos.column, msg)
    }

}