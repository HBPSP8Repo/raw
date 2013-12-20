package raw.algebra.emitter

import raw._
import raw.algebra._

import play.api.libs.json._
//import play.api.libs.functional._

object JsonEmitter {

  def expressionType(t: ExpressionType): JsValue = Json.toJson(t match {
    case BoolType => Map("type" -> Json.toJson("bool"))
    case FloatType => Map("type" -> Json.toJson("float"))
    case IntType => Map("type" -> Json.toJson("int"))
    case StringType => Map("type" -> Json.toJson("string"))
    case RecordType(atts) => Map("type" -> Json.toJson("record"), "v" -> Json.toJson(atts.map(att => Json.toJson(Map("name" -> Json.toJson(att.name), "type" -> Json.toJson(expressionType(att.expressionType)))))))
  })
  
  def binaryOp(op: BinaryOperator) = Json.toJson(op match {
    case op : Eq => "eq"
    case op : Neq => "neq"
    case op : Ge => "ge"
    case op : Gt => "gt"
    case op : Le => "le"
    case op : Lt => "lt"
    case op : Add => "add"
    case op : Sub => "sub"
    case op : Mult => "mult"
    case op : Div => "div"
  })
  
  def merge(m: PrimitiveMonoid) = Json.toJson(m match {
    case SumMonoid => "sum"
    case MultiplyMonoid => "multiply"
    case MaxMonoid => "max"
    case OrMonoid => "or"
    case AndMonoid => "and"
  })
  
  def expression(e: Expression): JsValue = Json.toJson(e match {
    case BoolConst(v) => Map("expression" -> Json.toJson("bool"), "v" -> Json.toJson(v))
    case IntConst(v) => Map("expression" -> Json.toJson("int"), "v" -> Json.toJson(v))
    case FloatConst(v) => Map("expression" -> Json.toJson("float"), "v" -> Json.toJson(v))
    case StringConst(v) => Map("expression" -> Json.toJson("string"), "v" -> Json.toJson(v))
    case Argument(t, id) => Map("expression" -> Json.toJson("argument"), "type" -> expressionType(t), "id" -> Json.toJson(id))
    case RecordProjection(t, e, name) => Map("expression" -> Json.toJson("recordProjection"), "type" -> expressionType(t), "e" -> expression(e), "name" -> Json.toJson(name))
    case RecordConstruction(t, atts) => Map("expression" -> Json.toJson("recordConstruction"), "type" -> expressionType(t), "atts" -> Json.toJson(atts.map(att => Json.toJson(Map("name" -> Json.toJson(att.name), "e" -> expression(att.e))))))
    case IfThenElse(t, e1, e2, e3) => Map("expression" -> Json.toJson("if"), "type" -> expressionType(t), "cond" -> expression(e1), "then" -> expression(e2), "else" -> expression(e3))
    case BinaryOperation(t, op, e1, e2) => Map("expression" -> binaryOp(op), "type" -> expressionType(t), "left" -> expression(e1), "right" -> expression(e2))
    case MergeMonoid(t, m, e1, e2) => Map("expression" -> merge(m), "type" -> expressionType(t), "left" -> expression(e1), "right" -> expression(e2))
    case Not(e) => Map("expression" -> Json.toJson("not"), "e" -> expression(e))
  })
  
  def listArgs(l: List[Argument]): JsValue = Json.toJson(l.map(a => expression(a)))

  def path(p: Path): JsValue = { 
    def getNames(p: Path): List[String] = p match {
      case a : ArgumentPath => List()
      case InnerPath(p, name) => getNames(p) ++ List(name)
    }
    
    def getVariable(p: Path): Int = p match {
      case ArgumentPath(id) => id
      case InnerPath(p, _) => getVariable(p)
    }
    
    Json.toJson(Map("argument" -> Json.toJson(getVariable(p)), "path" -> Json.toJson(getNames(p))))
  }
  
  def accumulator(m: Monoid) = Json.toJson(m match {
    case SumMonoid => "sum"
    case MultiplyMonoid => "multiply"
    case MaxMonoid => "max"
    case OrMonoid => "or"
    case AndMonoid => "and"
    case SetMonoid => "set"
    case BagMonoid => "bag"
    case ListMonoid => "list"
  })
  
  def emit(a: Algebra): JsValue = Json.toJson(a match {
    case Scan(name) => Map("operator" -> Json.toJson("scan"),
                           "plugin" -> Json.toJson(name))
    case Reduce(m, e, p, x) => Map("operator" -> Json.toJson("reduce"),
                                   "accumulator" -> accumulator(m),
                                   "e" -> expression(e),
                                   "p" -> expression(p),
                                   "input" -> emit(x))
    case Nest(m, e, f, p, g, x) => Map("operator" -> Json.toJson("nest"),
                                       "e" -> expression(e),
                                       "f" -> listArgs(f),
                                       "p" -> expression(p),
                                       "g" -> listArgs(g),
                                       "input" -> emit(x))
    case Select(p, x) => Map("operator" -> Json.toJson("select"),
                             "p" -> expression(p),
                             "input" -> emit(x))
    case Join(p, x, y) => Map("operator" -> Json.toJson("join"),
                              "lhs" -> emit(x),
                              "rhs" -> emit(y))
    case Unnest(path1, p, x) => Map("operator" -> Json.toJson("unnest"),
                                   "path" -> path(path1),
                                   "p" -> expression(p),
                                   "input" -> emit(x))
    case OuterJoin(p, x, y) => Map("operator" -> Json.toJson("outerJoin"),
                                   "lhs" -> emit(x),
                                   "rhs" -> emit(y))
    case OuterUnnest(path1, p, x) => Map("operator" -> Json.toJson("outerUnnest"),
                                        "path" -> path(path1),
                                        "p" -> expression(p),
                                        "input" -> emit(x))
    case Empty => throw RawInternalException("Empty node found")
  })
}


