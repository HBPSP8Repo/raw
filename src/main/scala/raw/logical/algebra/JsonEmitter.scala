package raw.logical.algebra

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithBigDecimal._
import raw._
import raw.logical._

object JsonEmitter {

  def apply(a: Algebra, cat: Catalog) = {

    def recurse(a: Algebra): JObject = {
      
      def monoidType(t: MonoidType): JObject = t match {
        case BoolType => ("type" -> "bool")
        case FloatType => ("type" -> "float")
        case IntType => ("type" -> "int")
        case StringType => ("type" -> "string")
        case RecordType(atts) => ("type" -> "record") ~ ("v" -> atts.map(att => ("name" -> att.name) ~ monoidType(att.monoidType)))
        case SetType(t) => ("type" -> "set") ~ ("inner" -> monoidType(t))
        case BagType(t) => ("type" -> "bag") ~ ("inner" -> monoidType(t))
        case ListType(t) => ("type" -> "list") ~ ("inner" -> monoidType(t))
      }
  
      def binaryOp(op: BinaryOperator) = op match {
        case Eq => "eq"
        case Neq => "neq"
        case Ge => "ge"
        case Gt => "gt"
        case Le => "le"
        case Lt => "lt"
        case Add => "add"
        case Sub => "sub"
        case Mult => "mult"
        case Div => "div"
      }
  
      def monoid(m: Monoid) = m match {
        case SumMonoid => "sum"
        case MultiplyMonoid => "multiply"
        case MaxMonoid => "max"
        case OrMonoid => "or"
        case AndMonoid => "and"
        case SetMonoid => "set"
        case BagMonoid => "bag"
        case ListMonoid => "list"
      }
  
      def expression(e: Expression): JObject = e match {
        case BoolConst(v) => ("expression" -> "bool") ~ ("v" -> v)
        case IntConst(v) => ("expression" -> "int") ~ ("v" -> v)
        case FloatConst(v) => ("expression" -> "float") ~ ("v" -> v)
        case StringConst(v) => ("expression" -> "string") ~ ("v" -> v)
        case Argument(t, id) => ("expression" -> "argument") ~ monoidType(t) ~ ("id" -> id)
        case RecordProjection(t, e, name) => ("expression" -> "recordProjection") ~ monoidType(t) ~ ("e" -> expression(e)) ~ ("name" -> name)
        case RecordConstruction(t, atts) => ("expression" -> "recordConstruction") ~ monoidType(t) ~ ("atts" -> atts.map(att => ("name" -> att.name) ~ ("e" -> expression(att.e))))
        case IfThenElse(t, e1, e2, e3) => ("expression" -> "if") ~ monoidType(t) ~ ("cond" -> expression(e1)) ~ ("then" -> expression(e2)) ~ ("else" -> expression(e3))
        case BinaryOperation(t, op, e1, e2) => ("expression" -> binaryOp(op)) ~ monoidType(t) ~ ("left" -> expression(e1)) ~ ("right" -> expression(e2))
        case MergeMonoid(t, m, e1, e2) => ("expression" -> monoid(m)) ~ monoidType(t) ~ ("left" -> expression(e1)) ~ ("right" -> expression(e2))
        case Not(e) => ("expression" -> "not") ~ ("e" -> expression(e))
        case FloatToInt(e) => ("expression" -> "floatToInt") ~ ("e" -> expression(e))
        case FloatToString(e) => ("expression" -> "floatToString") ~ ("e" -> expression(e))
        case IntToFloat(e) => ("expression" -> "intToFloat") ~ ("e" -> expression(e))
        case IntToString(e) => ("expression" -> "intToString") ~ ("e" -> expression(e))
        case StringToBool(e) => ("expression" -> "stringToBool") ~ ("e" -> expression(e))
        case StringToInt(e) => ("expression" -> "stringToInt") ~ ("e" -> expression(e))
        case StringToFloat(e) => ("expression" -> "stringToFloat") ~ ("e" -> expression(e))
      }
  
      def listArgs(l: List[Argument]) = l.map(a => expression(a))
  
      def path(p: Path) = {
        def getNames(p: Path): List[String] = p match {
          case a: ArgumentPath => List()
          case InnerPath(p, name) => getNames(p) ++ List(name)
        }
  
        def getVariable(p: Path): Int = p match {
          case ArgumentPath(id) => id
          case InnerPath(p, _) => getVariable(p)
        }
  
        ("argument" -> getVariable(p)) ~ ("path" -> getNames(p))
      }
  
      a match {
        case Scan(name) =>
          ("operator" -> "scan") ~
          ("plugin" -> name) ~
          monoidType(cat.getType(name))
        case Reduce(m, e, p, x) => 
          ("operator" -> "reduce") ~
          ("accumulator" -> monoid(m)) ~
          ("e" -> expression(e)) ~
          ("p" -> expression(p)) ~
          ("input" -> recurse(x))
        case Nest(m, e, f, p, g, x) =>
          ("operator" -> "nest") ~
          ("accumulator" -> monoid(m)) ~
          ("e" -> expression(e)) ~
          ("f" -> listArgs(f)) ~
          ("p" -> expression(p)) ~
          ("g" -> listArgs(g)) ~
          ("input" -> recurse(x))
        case Select(p, x) =>
          ("operator" -> "select") ~
          ("p" -> expression(p)) ~
          ("input" -> recurse(x))
        case Join(p, x, y) =>
          ("operator" -> "join") ~
          ("p" -> expression(p)) ~
          ("left" -> recurse(x)) ~
          ("right" -> recurse(y))
        case Unnest(path1, p, x) => 
          ("operator" -> "unnest") ~
          ("path" -> path(path1)) ~
          ("p" -> expression(p)) ~
          ("input" -> recurse(x))
        case OuterJoin(p, x, y) => 
          ("operator" -> "outerJoin") ~
          ("p" -> expression(p)) ~
          ("left" -> recurse(x)) ~
          ("right" -> recurse(y))
        case OuterUnnest(path1, p, x) =>
          ("operator" -> "outerUnnest") ~
          ("path" -> path(path1)) ~
          ("p" -> expression(p)) ~
          ("input" -> recurse(x))
        case Merge(m, x, y) => 
          ("operator" -> "merge") ~
          ("accumulator" -> monoid(m)) ~
          ("left" -> recurse(x)) ~
          ("right" -> recurse(y))
        case Empty => throw RawInternalException("Empty node found")
      }
    }
    pretty(render(recurse(a)))
  } 
}