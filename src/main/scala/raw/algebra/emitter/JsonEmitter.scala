//package raw.algebra.emitter
//
//import raw._
//import raw.algebra._
//
//object JsonEmitter {
//
//  def expressionType(t: ExpressionType) = t match {
//    case BoolType => Map("type" -> "bool")
//    case FloatType => Map("type" -> "float")
//    case IntType => Map("type" -> "int")
//    case StringType => Map("type" -> "string")
//    case RecordType(atts) => Map("type" -> "record", "v" -> atts.map(att => Map("name" -> att.name, "type" -> expressionType(att.expressionType))))
//  }
//
//  def binaryOp(op: BinaryOperator) = op match {
//    case op : Eq => "eq"
//    case op : Neq => "neq"
//    case op : Ge => "ge"
//    case op : Gt => "gt"
//    case op : Le => "le"
//    case op : Lt => "lt"
//    case op : Add => "add"
//    case op : Sub => "sub"
//    case op : Mult => "mult"
//    case op : Div => "div"
//  }
//
//  def merge(m: PrimitiveMonoid) = m match {
//    case SumMonoid => "sum"
//    case MultiplyMonoid => "multiply"
//    case MaxMonoid => "max"
//    case OrMonoid => "or"
//    case AndMonoid => "and"
//  }
//
//  def expression(e: Expression) = e match {
//    case BoolConst(v) => Map("expression" -> "bool", "v" -> v)
//    case IntConst(v) => Map("expression" -> "int", "v" -> v)
//    case FloatConst(v) => Map("expression" -> "float", "v" -> v)
//    case StringConst(v) => Map("expression" -> "string", "v" -> v)
//    case Argument(t, id) => Map("expression" -> "argument", "type" -> expressionType(t), "id" -> id)
//    case RecordProjection(t, e, name) => Map("expression" -> "recordProjection", "type" -> expressionType(t), "e" -> expression(e), "name" -> name)
//    case RecordConstruction(t, atts) => Map("expression" -> "recordConstruction", "type" -> expressionType(t), "atts" -> atts.map(att => Map("name" -> att.name, "e" -> expression(att.e))))
//    case IfThenElse(t, e1, e2, e3) => Map("expression" -> "if", "type" -> expressionType(t), "cond" -> expression(e1), "then" -> expression(e2), "else" -> expression(e3))
//    case BinaryOperation(t, op, e1, e2) => Map("expression" -> binaryOp(op), "type" -> expressionType(t), "left" -> expression(e1), "right" -> expression(e2))
//    case MergeMonoid(t, m, e1, e2) => Map("expression" -> merge(m), "type" -> expressionType(t), "left" -> expression(e1), "right" -> expression(e2))
//    case Not(e) => Map("expression" -> "not", "e" -> expression(e))
//  }
//
//  def listArgs(l: List[Argument]) = l.map(a => expression(a))
//
//  def path(p: Path) = {
//    def getNames(p: Path): List[String] = p match {
//      case a : ArgumentPath => List()
//      case InnerPath(p, name) => getNames(p) ++ List(name)
//    }
//
//    def getVariable(p: Path): Int = p match {
//      case ArgumentPath(id) => id
//      case InnerPath(p, _) => getVariable(p)
//    }
//
//    Map("argument" -> getVariable(p), "path" -> getNames(p))
//  }
//
//  def accumulator(m: Monoid) = m match {
//    case SumMonoid => "sum"
//    case MultiplyMonoid => "multiply"
//    case MaxMonoid => "max"
//    case OrMonoid => "or"
//    case AndMonoid => "and"
//    case SetMonoid => "set"
//    case BagMonoid => "bag"
//    case ListMonoid => "list"
//  }
//
//  def emit(a: Algebra) = a match {
//    case Scan(name) => Map("operator" -> "scan",
//                           "plugin" -> name)
//    case Reduce(m, e, p, x) => Map("operator" -> "reduce",
//                                   "accumulator" -> accumulator(m),
//                                   "e" -> expression(e),
//                                   "p" -> expression(p),
//                                   "input" -> emit(x))
//    case Nest(m, e, f, p, g, x) => Map("operator" -> "nest",
//                                       "accumulator" -> accumulator(m),
//                                       "e" -> expression(e),
//                                       "f" -> listArgs(f),
//                                       "p" -> expression(p),
//                                       "g" -> listArgs(g),
//                                       "input" -> emit(x))
//    case Select(p, x) => Map("operator" -> "select",
//                             "p" -> expression(p),
//                             "input" -> emit(x))
//    case Join(p, x, y) => Map("operator" -> "join",
//                              "p" -> expression(p),
//                              "left" -> emit(x),
//                              "right" -> emit(y))
//    case Unnest(path1, p, x) => Map("operator" -> "unnest",
//                                   "path" -> path(path1),
//                                   "p" -> expression(p),
//                                   "input" -> emit(x))
//    case OuterJoin(p, x, y) => Map("operator" -> "outerJoin",
//                                   "p" -> expression(p),
//                                   "left" -> emit(x),
//                                   "right" -> emit(y))
//    case OuterUnnest(path1, p, x) => Map("operator" -> "outerUnnest",
//                                        "path" -> path(path1),
//                                        "p" -> expression(p),
//                                        "input" -> emit(x))
//    case Empty => throw RawInternalException("Empty node found")
//  }
//}
//
//
