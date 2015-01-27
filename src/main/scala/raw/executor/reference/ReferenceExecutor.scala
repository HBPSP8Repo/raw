package raw.executor.reference

import com.typesafe.scalalogging.LazyLogging
import raw._
import algebra._
import PhysicalAlgebra._
import raw.executor.{RawExecutorRuntimeException, Executor}

import scala.io.BufferedSource

case class ReferenceExecutorError(err: String) extends RawException

class ReferenceResult(result: Value) extends QueryResult {

  private def toScala(value: Value): Any = value match {
    case NullValue() => null
    case IntValue(i) => i
    case FloatValue(f) => f
    case BooleanValue(b) => b
    case StringValue(s) => s
    case SetValue(s) => s.map(v => toScala(v))
    case ListValue(l) => l.map(v => toScala(v))
    case RecordValue(m: Map[String, Value]) => m.map(v => (v._1, toScala(v._2)))
  }

  val value = toScala(result)
}

object ReferenceExecutor extends Executor with LazyLogging {

  def valueToString(value: Value): Any = value match {
    case NullValue() => "null"
    case IntValue(i) => i.toString
    case FloatValue(f) => f.toString
    case BooleanValue(b) => b.toString
    case StringValue(s) => "\"" + s + "\""
    case SetValue(s) => "{" + s.map(v => valueToString(v)).mkString(", ") + "}"
    case ListValue(l) => "(" + l.map(v => valueToString(v)).mkString(", ") + ")"
    case RecordValue(m: Map[String, Value]) => "[" + m.map(v => v._1 + "=" + valueToString(v._2)).mkString(", ") + "]"
  }

  // helper log function. You pass an expression and an environment and it logs the value + returns it
  def returnedValue(exp: Exp, env: List[Value], value: Value): Value = {
    logger.debug("eval " + PhysicalAlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.map(valueToString).mkString(", ") + ") ===> " + valueToString(value))
    value
  }

  def execute(root: PhysicalAlgebra.AlgebraNode, world: World): Either[QueryError, QueryResult] = {

    def dataLocDecode(t: Type, data: Iterable[Any]): DataSource = {
        def recurse(t: Type, value: Any): Value = (t, value) match {
          case (IntType(), v: Int) => IntValue(v)
          case (FloatType(), v: Float) => FloatValue(v)
          case (BoolType(), v: Boolean) => BooleanValue(v)
          case (StringType(), v: String) => StringValue(v)
          case (r: RecordType, v: Map[_, _]) => v match { case x: Map[String, Value] => RecordValue(x.map({ c => (c._1, recurse(r.typeOf(c._1), x(c._1)))})) }
          case (CollectionType(ListMonoid(), what: Type), v: List[_]) => ListValue(v.map({ x: Any => recurse(what, x)}))
          case (CollectionType(SetMonoid(), what: Type), v: Set[_]) => SetValue(v.map({ x: Any => recurse(what, x)}))
          case _ => throw RawExecutorRuntimeException(s"cannot parse $t in $value")
        }
        recurse(t, data) match {
          case ListValue(l) => MemoryDataSource(l)
          //case SetValue(s) => MemoryDataSource(s) TODO: support sets? Can you do .next?
          case v => throw RawExecutorRuntimeException(s"cannot build a memory scan from $v")
        }
    }

    def loadCSV(t: Type, content: BufferedSource): DataSource = {

      def parse(t: Type, item: String): Value = t match {
        case IntType() => IntValue(item.toInt)
        case FloatType() => FloatValue(item.toFloat)
        case BoolType() => BooleanValue(item.toBoolean)
        case StringType() => StringValue(item)
        case RecordType(atts) => RecordValue(atts.zip(item.split(",")).map(_ match {case (a: AttrType, l: String) => (a.idn, parse(a.tipe, l))}).toMap)
        case _ => throw RawExecutorRuntimeException(s"cannot parse $t in text/csv files")
      }
      t match {
        case CollectionType(ListMonoid(), innerType) => MemoryDataSource(content.getLines().toList.map({ l: String => parse(innerType, l)}))
        case _ => throw RawExecutorRuntimeException(s"cannot make data source from $t")
      }
    }

    def toOperator(opNode: AlgebraNode): ScalaOperator = opNode match {
      case Join(ps, left, right) => JoinOperator(ps, toOperator(left), toOperator(right))
      case OuterJoin(ps, left, right) => OuterJoinOperator(ps, toOperator(left), toOperator(right))
      case Select(ps, source) => SelectOperator(ps, toOperator(source))
      case Nest(m, e, f, ps, g, child) => NestOperator(m, e, f, ps, g, toOperator(child))
      case _: OuterUnnest => ???
      case _: Unnest => ???
      case _: Merge => ???
      case Reduce(m, exp, ps, source) => ReduceOperator(m, exp, ps, toOperator(source))
      case Scan(tipe, loc) => loc match {
        case LocalFileLocation(path, "text/csv") => ScanOperator(loadCSV(tipe, scala.io.Source.fromFile(path)))
        case MemoryLocation(data)              => ScanOperator(dataLocDecode(tipe, data))
        case loc                               => throw ReferenceExecutorError(s"Reference executor does not support location $loc")
      }
    }

    logger.debug("\n==========\n" + root + "\n" + PhysicalAlgebraPrettyPrinter.pretty(root) + "\n============")
    val operator = toOperator(root)
    operator.next match {
      case r @ Some(v) => Right(new ReferenceResult(v.head))
      case None => throw RawExecutorRuntimeException(s"unexpected None from $root")
    }
  }

  private def evalPredicates(ps: List[Exp], items: List[Value]): Boolean = {
    val values = ps.map({p: Exp => expEval(p, items)}).map(toBool)
    values.forall({p: Boolean => p})
  }

  private def toBool(v: Value): Boolean = v match {
    case b: BooleanValue => b.value
    case _ => false
  }

  private def expEval(exp: Exp, env: List[Value]): Value = returnedValue(exp, env, exp match {
    case Null => NullValue()
    case BoolConst(v) => BooleanValue(v)
    case IntConst(v) => IntValue(v)
    case FloatConst(v) => FloatValue(v)
    case StringConst(v) => StringValue(v)
    case v: Arg => varEval(v, env)
    case RecordCons(attributes) => RecordValue(attributes.map(att => (att.idn, expEval(att.e, env))).toMap)
    case RecordProj(e, idn) => expEval(e, env) match {
      case v: RecordValue => v.value(idn)
      case _ => throw RawExecutorRuntimeException(s"cannot do a record projection from $e")
    }
    case ZeroCollectionMonoid(m) => zeroEval(m)
    case ConsCollectionMonoid(m: CollectionMonoid, e) => consCollectionEval(m)(expEval(e, env))
    case MergeMonoid(m: CollectionMonoid, e1, e2) => mergeEval(m)(expEval(e1, env), expEval(e2, env))
    case MergeMonoid(m: BoolMonoid, e1, e2) => mergeBoolEval(m)(expEval(e1, env), expEval(e2, env))
    case UnaryExp(op, e) => unaryOpEval(op)(expEval(e, env))
    case IfThenElse(e1, e2, e3) => if(expEval(e1, env) == BooleanValue(true)) expEval(e2, env) else expEval(e3, env)
    case BinaryExp(op, e1, e2) => binaryOpEval(op)(expEval(e1, env), expEval(e2, env))
  })


  private def varEval(v: Arg, env: List[Value]): Value = env(v.i)

  private def zeroEval(m: Monoid): Value = m match {
    case _: SetMonoid => SetValue(Set())
    case _: ListMonoid => ListValue(List())
    case _: BagMonoid => ???
    case _: AndMonoid => BooleanValue(true)
    case _: OrMonoid => BooleanValue(false)
    case _: SumMonoid => IntValue(0)
    case _: MultiplyMonoid => IntValue(1)
    case _: MaxMonoid => IntValue(0) // TODO
  }

  private def consCollectionEval(m: CollectionMonoid): Value => Value = m match {
    case _: SetMonoid => (e: Value) => SetValue(Set(e))
    case _: ListMonoid => (e: Value) => ListValue(List(e))
    case _: BagMonoid => (e: Value) => ???
  }

  private def mergeBoolEval(m: BoolMonoid): (Value, Value) => Value = m match {
    case b: AndMonoid => (v1: Value, v2: Value) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value && b2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot perform 'and' of $v1 and $v2")
    }
    case b: OrMonoid => (v1: Value, v2: Value) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value || b2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot perform 'or' of $v1 and $v2")
    }
  }

  private def mergeEval(m: CollectionMonoid): (Value, Value) => CollectionValue = m match {
    case _: ListMonoid => (e1: Value, e2: Value) => (e1, e2) match {
      case (l1: ListValue, l2: ListValue) => ListValue(l1.value ++ l2.value)
      case (l1: ListValue, l2: SetValue) => ListValue(l1.value ++ l2.value.toList)
      case (l1: SetValue, l2: SetValue) => ListValue(l1.value.toList ++ l2.value.toList)
      case (l1: SetValue, l2: ListValue) => ListValue(l1.value.toList ++ l2.value)
      case _ => throw RawExecutorRuntimeException(s"building a list from $e1 and $e2 is not supported")
    }
    case _: SetMonoid => (e1: Value, e2: Value) => (e1, e2) match {
      case (l1: SetValue, l2: SetValue) => SetValue(l1.value ++ l2.value)
      case _ => throw RawExecutorRuntimeException(s"building a set from $e1 and $e2 is not supported")
    }
    case _: BagMonoid => throw RawExecutorRuntimeException("bags are not supported")
  }

  private def unaryOpEval(op: UnaryOperator): Value => Value = op match {
    case _: Not => v: Value => v match {
      case b: BooleanValue => BooleanValue(!b.value)
      case i: IntValue => BooleanValue(i.value == 0)
      case f: FloatValue => BooleanValue(f.value == 0)
      case _ => throw RawExecutorRuntimeException(s"cannot compute not($v)")
    }
    case _: Neg => v: Value => v match {
      case i: IntValue => IntValue(-i.value)
      case f: FloatValue => FloatValue(-f.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute neg($v)")
    }
    case _: ToBool => v: Value => v match {
      case _: BooleanValue => v
      case i: IntValue => BooleanValue(i.value != 0)
      case f: FloatValue => BooleanValue(f.value!= 0)
      case _ => throw RawExecutorRuntimeException(s"cannot compute toBool($v)")
    }
    case _: ToInt  => v: Value => v match {
      case BooleanValue(true) => IntValue(1)
      case BooleanValue(false) => IntValue(0)
      case _: IntValue => v
      case f: FloatValue => IntValue(f.value.toInt)
      case _ => throw RawExecutorRuntimeException(s"cannot compute toInt($v)")
    }
    case _: ToFloat => v: Value => v match {
      case BooleanValue(true) => FloatValue(1.0f)
      case BooleanValue(false) => FloatValue(0.0f)
      case i: IntValue => FloatValue(i.value)
      case _: FloatValue => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toFloat($v)")
    }
    case _: ToString => v: Value => v match {
      case b: BooleanValue => StringValue(b.value.toString())
      case i: IntValue => StringValue(i.value.toString())
      case f: FloatValue => StringValue(f.value.toString())
      case s: StringValue => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toString($v)")
    }
  }

  private def binaryOpEval(op: BinaryOperator): (Value, Value) => Value = op match {
    case _: Eq => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value == e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value == e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value == e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value == e2.value)
      case (e1: BooleanValue, e2: BooleanValue) => BooleanValue(e1.value == e2.value)
      case (e1: StringValue, e2: StringValue) => BooleanValue(e1.value == e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute eq($i1, $i2)")
    }
    case _: Neq => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value != e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value != e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value != e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value != e2.value)
      case (e1: BooleanValue, e2: BooleanValue) => BooleanValue(e1.value != e2.value)
      case (e1: StringValue, e2: StringValue) => BooleanValue(e1.value != e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute neq($i1, $i2)")
    }
    case _: Lt => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value < e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value < e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value < e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value < e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute lt($i1, $i2)")
    }
    case _: Le => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value <= e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value <= e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value <= e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value <= e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute le($i1, $i2)")
    }
    case _: Ge => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value >= e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value >= e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value >= e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value >= e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute ge($i1, $i2)")
    }
    case _: Gt => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value > e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value > e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value > e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value > e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute gt($i1, $i2)")
    }
    case _: Div => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => IntValue(e1.value / e2.value)
      case (e1: IntValue, e2: FloatValue) => FloatValue(e1.value / e2.value)
      case (e1: FloatValue, e2: FloatValue) => FloatValue(e1.value / e2.value)
      case (e1: FloatValue, e2: IntValue) => FloatValue(e1.value / e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute div($i1, $i2)")
    }
    case _: Sub => (i1: Value, i2: Value) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => IntValue(e1.value - e2.value)
      case (e1: IntValue, e2: FloatValue) => FloatValue(e1.value - e2.value)
      case (e1: FloatValue, e2: FloatValue) => FloatValue(e1.value - e2.value)
      case (e1: FloatValue, e2: IntValue) => FloatValue(e1.value - e2.value)
      case _ => throw RawExecutorRuntimeException(s"cannot compute sub($i1, $i2)")
    }
  }

  abstract class ScalaOperator {
    def next: Option[List[Value]]

    def returnedValue(value: Option[List[Value]]): Option[List[Value]] = value match {
      case r@Some(v) => logger.debug("" + this + " ===> " + v.map(valueToString)); value
      case None => logger.debug("" + this + " ===> None"); value
    }
  }

  /** Join Operator
    */
  case class JoinOperator(ps: List[Exp], left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "join (" + ps.map(PhysicalAlgebraPrettyPrinter.pretty).mkString(" && ") + ") " + left + " X " + right

    private def readRight: List[List[Value]] = right.next match {
      case Some(v: List[Value]) => List(v) ++ readRight
      case None => List()
    }

    private val rightValues: List[List[Value]] = readRight

    def recurse: List[List[Value]] = left.next match {
      case r@Some(v: List[Value]) => (rightValues.filter({l: List[Value] => evalPredicates(ps, v ++ l)})).map{l: List[Value] => v ++ l} ++ recurse
      case None => List()
    }

    case class JoinOutput(listValue: List[List[Value]]) {
      private var index: Int = 0
      private val L: List[List[Value]] = listValue

      def next() = {
        val n = index
        index += 1
        returnedValue(try {
          Some(L(n))
        } catch {
          case ex: IndexOutOfBoundsException => None
        })
      }
    }

    private val matches = JoinOutput(recurse)

    def next = matches.next()
  }


  case class OuterJoinOperator(ps: List[Exp], left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "outer-join (" + ps.map(PhysicalAlgebraPrettyPrinter.pretty).mkString(" && ") + ") " + left + " X " + right

    private def readRight: List[List[Value]] = right.next match {
      case Some(v: List[Value]) => List(v) ++ readRight
      case None => List()
    }

    private val rightValues: List[List[Value]] = readRight

    def recurse: List[List[Value]] = left.next match {
      case r@Some(v: List[Value]) => {
        val matches = rightValues.filter({l: List[Value] => evalPredicates(ps, v ++ l)})
        if (matches == List())
          List(v ++ List(NullValue())) ++ recurse
        else matches.map({i: List[Value] => v ++ i}) ++ recurse
      }
      case None => List()
    }

    case class OuterJoinOutput(listValue: List[List[Value]]) {
      private var index: Int = 0
      private val L: List[List[Value]] = listValue

      def next() = {
        val n = index
        index += 1
        returnedValue(try {
          Some(L(n))
        } catch {
          case ex: IndexOutOfBoundsException => None
        })
      }
    }
    private val matches = OuterJoinOutput(recurse)

    def next = matches.next()

  }



  /** Select Operator
    */
  case class SelectOperator(ps: List[Exp], source: ScalaOperator) extends ScalaOperator {

    override def toString() = "select (" + ps.map(PhysicalAlgebraPrettyPrinter.pretty).mkString(" && ") + ") " + source

    private val child = source
    def recurse: Option[List[Value]] = child.next match {
      case r@Some(v) => if(evalPredicates(ps, v)) returnedValue(r) else recurse
      case None => returnedValue(None)
    }

    def next = recurse
  }

  case class NestOperator(m: Monoid, e: Exp, f: List[Arg], ps: List[Exp], g: List[Arg], source: ScalaOperator) extends ScalaOperator {

    override def toString() = "nest (" + List(m, PhysicalAlgebraPrettyPrinter.pretty(e), f, ps.map(PhysicalAlgebraPrettyPrinter.pretty).mkString(" && "), g, source).mkString(", ") + ")"

    private def readSource: List[List[Value]] = source.next match {
      case Some(v: List[Value]) => List(v) ++ readSource
      case None => List()
    }

    // map from groupBy variables to list of elements
    // the groupBy value is the list of arguments in "f" evaluated against each value read from the source
    private val valuesS: Map[List[Value], List[List[Value]]] = readSource.groupBy({l => f.map({v => expEval(v, l)})})
    // list of elements should be replaced by the list of values of e if g doesn't show null values
    //private val valuesE: Map[List[Value], List[Value]] = valuesS.map(p => (p._1, p._2.map({_ => expEval(e, _)})))

    // returns true if any of the variables in 'g' evaluates to NullValue.
    def skip(env: List[Value]): Boolean = {
      g.map{v: Arg => expEval(v, env)}.exists({x: Value => x == NullValue()})
    }

    private val valuesE: Map[List[Value], List[Value]] = valuesS.map(p => (p._1, p._2.map({x: List[Value] => if (skip(x)) zeroEval(m) else expEval(e, x)})))
    // list of values of e should be reduced according to the monoid
    private val values = valuesE.map(p => (p._1, p._2.foldLeft(zeroEval(m))({(x: Value, y: Value) => doReduce(m, x, y)})))
    // make the list of list
    //private val listOfList: List[List[Value]] = values.toList.map{p => List(ListValue(p._1), p._2)}
    private val listOfList: List[List[Value]] = values.toList.map{p => p._1 ++ List(p._2)}

    case class NestOutput(listValue: List[List[Value]]) {
      private var index: Int = 0
      private val L: List[List[Value]] = listValue

      def next() = {
        val n = index
        index += 1
        returnedValue(try { Some(L(n)) } catch { case ex: IndexOutOfBoundsException => None })
      }
    }

    private val matches = NestOutput(listOfList)


    def next = matches.next()

  }

  private def doReduce(m: Monoid, v1: Value, v2: Value): Value = (m, v1, v2) match {
    case (_: ListMonoid, v: Value, l: ListValue) => ListValue(List(v) ++ l.value)
    case (_: SetMonoid, v: Value, s: SetValue) => SetValue(Set(v) ++ s.value)
    case (_: AndMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
    case (_: OrMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
    case (_: SumMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value + v2.value)
    case (_: MultiplyMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value * v2.value)
    case (_: MaxMonoid, v1: IntValue, v2: IntValue) => IntValue(math.max(v1.value, v2.value))
    case _ => throw RawExecutorRuntimeException(s"cannot reduce($m, $v1, $v2)")
  }

  /** Reduce Operator
    */
  case class ReduceOperator(m: Monoid, exp: Exp, ps: List[Exp], source: ScalaOperator) extends ScalaOperator {

    override def toString() = "reduce (" + List(m, PhysicalAlgebraPrettyPrinter.pretty(exp), ps.map(PhysicalAlgebraPrettyPrinter.pretty).mkString(" && "), source).mkString(", ") + ")"

    def next = {
      def recurse: Value = source.next match {
        case None => zeroEval(m)
        case r@Some(v) => doReduce(m, expEval(exp, v), recurse)
      }
      returnedValue(Some(List(recurse)))
    }
  }

  /** Scan Operator
    */
  case class ScanOperator(d: DataSource) extends ScalaOperator {
    override def toString() = "scan()"
    private val source = d
    def next = returnedValue(d.next())
  }

}


/*
abstract class Operator() {
  def next(): List[MyValue]
}

class DataSource(listValue: List[MyValue]) {

  private var index: Int = 0
  private val L: List[MyValue] = listValue

  def next() = {
    val n = index
    index += 1
    try {
      Some(List(L(n)))
    } catch {
      case ex: IndexOutOfBoundsException => None
    }
  }
}

case class ScanOperator(s: Scan) extends Operator {
  val datasource = dataSources(s.name)
}

class ScalaExecutor(classes: Map[String, ListValue]) extends Executor(classes) {

  val dataSources: Map[String, DataSource] = classes.map({ x => (x._1, new DataSource(x._2.value))})

  def execute(opNode: OperatorNode): List[MyValue] = {
    def recurse: List[MyValue] = next(opNode) match {
      case r@Some(v) => v ++ recurse
      case None => List()
    }
    recurse
  }

  private def toBool(v: MyValue): Boolean = v match {
    case b: BooleanValue => b.value
    case _ => false
  }

  private def evalPredicates(ps: List[Exp], items: List[MyValue]): Boolean = {
    val values = ps.map({p: Exp => expEval(p, items)}).map(toBool)
    values.forall({p: Boolean => p})
  }

  def next(opNode: OperatorNode): Option[List[MyValue]] = opNode match {

    case s: Scan => dataSources(s.name).next()

    case Select(ps, child) => {
      def recurse: Option[List[MyValue]] = next(child) match {
        case r@Some(v) => if (evalPredicates(ps, v)) r else recurse
        case None => None
      }
      recurse
    }

    case Reduce(m: Monoid, e, ps, child) => {
      def recurse: Option[MyValue] = next(child) match {
        case r@Some(v) => {
          val n = recurse
          if (evalPredicates(ps, v)) {
            n match {
              case rN@Some(vN) => Some(mergeReduce(m, expEval(e, v), vN))
              case None => Some(mergeReduce(m, expEval(e, v), zeroEval(m)))
            }
          } else {
            n match {
              case rN@Some(vN) => rN
              case None => Some(zeroEval(m))
            }
          }
        }
        case None => None
      }
      recurse match {
        case Some(v) => Some(List(v))
        case None => None
      }
    }

    case Join(ps: List[Exp], left: OperatorNode, right: OperatorNode) =>
      // first read all left node into an Option[List[MyValue]]
      def readRight: List[List[MyValue]] = next(right) match {
        case Some(v: List[MyValue]) => List(v) ++ readRight
        case None => List()
      }

      val rightList = readRight

      def recurse: Option[List[MyValue]] = next(left) match {
        case Some(v: List[MyValue]) => {

        }
        case None => None
      }

  }

  def mergeBoolReduce(m: BoolMonoid): (MyValue, MyValue) => MyValue = m match {
    case b: AndMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value && b2.value)
    }
    case b: OrMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value || b2.value)
    }
  }

  def mergeReduce(m: Monoid, v1: MyValue, v2: MyValue): MyValue = (m, v1, v2) match {
    case (_: ListMonoid,v: MyValue, l: ListValue) => ListValue(List(v) ++ l.value)
    case (_: SetMonoid, v: MyValue, s: SetValue) => SetValue(Set(v) ++ s.value)
    case (_: AndMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
    case (_: OrMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
    case (_: SumMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value + v2.value)
    case (_: MultiplyMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value * v2.value)
  }

  def expEval(exp: Exp, env: List[MyValue]): MyValue = exp match {
    case BoolConst(v) => BooleanValue(v)
    case IntConst(v) => IntValue(v)
    case FloatConst(v) => FloatValue(v)
    case StringConst(v) => StringValue(v)
    case v: Arg => varEval(v, env)
    case RecordCons(attributes) => RecordValue(attributes.map(att => (att.idn, expEval(att.e, env))).toMap)
    case RecordProj(e, idn) => expEval(e, env) match { case v: RecordValue => v.value(idn) }
    case ZeroCollectionMonoid(m) => zeroCollectionEval(m)
    case ConsCollectionMonoid(m: CollectionMonoid, e) => consCollectionEval(m)(expEval(e, env))
    case MergeMonoid(m: CollectionMonoid, e1, e2) => mergeEval(m)(expEval(e1, env), expEval(e2, env))
    case MergeMonoid(m: BoolMonoid, e1, e2) => mergeBoolEval(m)(expEval(e1, env), expEval(e2, env))
    case UnaryExp(op, e) => unaryOpEval(op)(expEval(e, env))
    case IfThenElse(e1, e2, e3) => if(expEval(e1, env) == BooleanValue(true)) expEval(e2, env) else expEval(e3, env)
    case BinaryExp(op, e1, e2) => binaryOpEval(op)(expEval(e1, env), expEval(e2, env))
  }


  def varEval(v: Arg, env: List[MyValue]): MyValue = {
    env(v.i)
  }

  def zeroCollectionEval(m: CollectionMonoid): CollectionValue = m match {
    case _: SetMonoid => SetValue(Set())
    case _: ListMonoid => ListValue(List())
    case _: BagMonoid => ???
    case _: AndMonoid => BooleanValue(true)
    case _: OrMonoid => BooleanValue(false)
    case _: SumMonoid => IntValue(0)
    case _: MultiplyMonoid => IntValue(1)
  }

  def consCollectionEval(m: CollectionMonoid): MyValue => MyValue = m match {
    case _: SetMonoid => (e: MyValue) => SetValue(Set(e))
    case _: ListMonoid => (e: MyValue) => ListValue(List(e))
    case _: BagMonoid => (e: MyValue) => ???
  }

  def mergeBoolEval(m: BoolMonoid): (MyValue, MyValue) => MyValue = m match {
    case b: AndMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value && b2.value)
    }
    case b: OrMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value || b2.value)
    }
  }

  def mergeEval(m: CollectionMonoid): (MyValue, MyValue) => MyValue = m match {
    case _: ListMonoid => (e1: MyValue, e2: MyValue) => (e1, e2) match {
      case (l1: ListValue, l2: ListValue) => ListValue(l1.value ++ l2.value)
      case (l1: ListValue, l2: SetValue) => ListValue(l1.value ++ l2.value.toList)
      case (l1: SetValue, l2: SetValue) => ListValue(l1.value.toList ++ l2.value.toList)
      case (l1: SetValue, l2: ListValue) => ListValue(l1.value.toList ++ l2.value)
    }
    case _: SetMonoid => (e1: MyValue, e2: MyValue) => (e1, e2) match {
      case (l1: SetValue, l2: SetValue) => SetValue(l1.value ++ l2.value)
    }
  }

  def unaryOpEval(op: UnaryOperator): MyValue => MyValue = op match {
    case _: Not => v: MyValue => v match {
      case b: BooleanValue => BooleanValue(!b.value)
      case i: IntValue => BooleanValue(i.value == 0)
      case f: FloatValue => BooleanValue(f.value == 0)
    }
    case _: Neg => v: MyValue => v match {
      case i: IntValue => IntValue(-i.value)
      case f: FloatValue => FloatValue(-f.value)
    }
    case _: ToBool => v: MyValue => v match {
      case _: BooleanValue => v
      case i: IntValue => BooleanValue(i.value != 0)
      case f: FloatValue => BooleanValue(f.value!= 0)
    }
    case _: ToInt  => v: MyValue => v match {
      case BooleanValue(true) => IntValue(1)
      case BooleanValue(false) => IntValue(0)
      case _: IntValue => v
      case f: FloatValue => IntValue(f.value.toInt)
    }
    case _: ToFloat => v: MyValue => v match {
      case BooleanValue(true) => FloatValue(1.0f)
      case BooleanValue(false) => FloatValue(0.0f)
      case i: IntValue => FloatValue(i.value)
      case _: FloatValue => v
    }
    case _: ToString => v: MyValue => v match {
      case b: BooleanValue => StringValue(b.value.toString())
      case i: IntValue => StringValue(i.value.toString())
      case f: FloatValue => StringValue(f.value.toString())
      case s: StringValue => v
    }
  }

  def binaryOpEval(op: BinaryOperator): (MyValue, MyValue) => MyValue = op match {
    case _: Eq => (i1: MyValue, i2: MyValue) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value == e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value == e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value == e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value == e2.value)
      case (e1: BooleanValue, e2: BooleanValue) => BooleanValue(e1.value == e2.value)
      case (e1: StringValue, e2: StringValue) => BooleanValue(e1.value == e2.value)
    }
    case _: Neq => (i1: MyValue, i2: MyValue) => (i1, i2) match {
      case (e1: IntValue, e2: IntValue) => BooleanValue(e1.value != e2.value)
      case (e1: IntValue, e2: FloatValue) => BooleanValue(e1.value != e2.value)
      case (e1: FloatValue, e2: FloatValue) => BooleanValue(e1.value != e2.value)
      case (e1: FloatValue, e2: IntValue) => BooleanValue(e1.value != e2.value)
      case (e1: BooleanValue, e2: BooleanValue) => BooleanValue(e1.value != e2.value)
      case (e1: StringValue, e2: StringValue) => BooleanValue(e1.value != e2.value)
    }
  }
}


*/