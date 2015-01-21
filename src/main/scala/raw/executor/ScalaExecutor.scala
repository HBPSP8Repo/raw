package raw.executor

import raw._
import raw.algebra.Algebra._
import raw.calculus.World

/**
 * Created by gaidioz on 1/14/15.
 */

class ScalaExecutor(schema: World, dataLocations: Map[String, DataLocation]) extends Executor(schema, dataLocations) {

  def execute(operatorNode: OperatorNode): MyValue = {

    val operator = toOperator(operatorNode)
    operator.next match {
      case r@Some(v) => v.head
    }
  }

  abstract class ScalaOperator {
    def next: Option[List[MyValue]]
  }

  case class JoinOperator(ps: List[Exp], left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    private def readLeft: List[List[MyValue]] = left.next match {
      case Some(v: List[MyValue]) => List(v) ++ readLeft
      case None => List()
    }

    private val leftValues: List[List[MyValue]] = readLeft

    def recurse: List[List[MyValue]] = right.next match {
      case r@Some(v) => leftValues.filter({l: List[MyValue] => evalPredicates(ps, l)}) ++ recurse
      case None => List()
    }

    case class JoinOutput(listValue: List[List[MyValue]]) {
      private var index: Int = 0
      private val L: List[List[MyValue]] = listValue

      def next() = {
        val n = index
        index += 1
        try {
          Some(L(n))
        } catch {
          case ex: IndexOutOfBoundsException => None
        }
      }
    }

    private val matches = JoinOutput(recurse)

    def next = matches.next()
  }

  case class SelectOperator(ps: List[Exp], source: ScalaOperator) extends ScalaOperator {
    private val child = source
    def recurse: Option[List[MyValue]] = child.next match {
      case r@Some(v) => if(evalPredicates(ps, v)) r else recurse
      case None => None
    }

    def next = recurse
  }
  case class ReduceOperator(m: Monoid, exp: Exp, ps: List[Exp], source: ScalaOperator) extends ScalaOperator {

    private def reduce(m: Monoid, v1: MyValue, v2: MyValue): MyValue = (m, v1, v2) match {
      case (_: ListMonoid, v: MyValue, l: ListValue) => ListValue(List(v) ++ l.value)
      case (_: SetMonoid, v: MyValue, s: SetValue) => SetValue(Set(v) ++ s.value)
      case (_: AndMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
      case (_: OrMonoid, v1: BooleanValue, v2: BooleanValue) => BooleanValue(v1.value && v2.value)
      case (_: SumMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value + v2.value)
      case (_: MultiplyMonoid, v1: IntValue, v2: IntValue) => IntValue(v1.value * v2.value)
      case (_: MaxMonoid, v1: IntValue, v2: IntValue) => IntValue(math.max(v1.value, v2.value))
    }

    def next = {
      def recurse: MyValue = source.next match {
        case None => zeroEval(m)
        case r@Some(v) => reduce(m, expEval(exp, v), recurse)
      }
      Some(List(recurse))
    }
  }

  case class ScanOperator(d: DataSource) extends ScalaOperator {
    private val source = d
    def next = d.next()
  }

  private def dataLocDecode(t: Type, dataLocation: DataLocation): DataSource = dataLocation match {
    case MemoryLocation(source) => {
      def recurse(t: Type, value: Any): MyValue = (t, value) match {
        case (IntType(), v: Int) => IntValue(v)
        case (FloatType(), v: Float) => FloatValue(v)
        case (BoolType(), v: Boolean) => BooleanValue(v)
        case (StringType(), v: String) => StringValue(v)
        case (r: RecordType, v: Map[String, Any]) => RecordValue(v.map({ c => (c._1, recurse(r.typeOf(c._1), v(c._1)))}))
        case (CollectionType(ListMonoid(), what: Type), v: List[Any]) => ListValue(v.map({ x: Any => recurse(what, x)}))
      }
      recurse(t, source) match {
        case ListValue(l) => MemoryDataSource(l)
      }
    }
  }

  private def toOperator(opNode: OperatorNode): ScalaOperator = opNode match {
    case Join(ps, left, right) => JoinOperator(ps, toOperator(left), toOperator(right))
    case Select(ps, source) => SelectOperator(ps, toOperator(source))
    case Reduce(m, exp, ps, source) => ReduceOperator(m, exp, ps, toOperator(source))
    case Scan(s) => ScanOperator(dataLocDecode(schema.typeOf(s), dataLocations(s)))
  }

  private def evalPredicates(ps: List[Exp], items: List[MyValue]): Boolean = {
    val values = ps.map({p: Exp => expEval(p, items)}).map(toBool)
    values.forall({p: Boolean => p})
  }

  private def toBool(v: MyValue): Boolean = v match {
    case b: BooleanValue => b.value
    case _ => false
  }

  private def expEval(exp: Exp, env: List[MyValue]): MyValue = exp match {
    case BoolConst(v) => BooleanValue(v)
    case IntConst(v) => IntValue(v)
    case FloatConst(v) => FloatValue(v)
    case StringConst(v) => StringValue(v)
    case v: Arg => varEval(v, env)
    case RecordCons(attributes) => RecordValue(attributes.map(att => (att.idn, expEval(att.e, env))).toMap)
    case RecordProj(e, idn) => expEval(e, env) match { case v: RecordValue => v.value(idn) }
    case ZeroCollectionMonoid(m) => zeroEval(m)
    case ConsCollectionMonoid(m: CollectionMonoid, e) => consCollectionEval(m)(expEval(e, env))
    case MergeMonoid(m: CollectionMonoid, e1, e2) => mergeEval(m)(expEval(e1, env), expEval(e2, env))
    case MergeMonoid(m: BoolMonoid, e1, e2) => mergeBoolEval(m)(expEval(e1, env), expEval(e2, env))
    case UnaryExp(op, e) => unaryOpEval(op)(expEval(e, env))
    case IfThenElse(e1, e2, e3) => if(expEval(e1, env) == BooleanValue(true)) expEval(e2, env) else expEval(e3, env)
    case BinaryExp(op, e1, e2) => binaryOpEval(op)(expEval(e1, env), expEval(e2, env))
  }


  private def varEval(v: Arg, env: List[MyValue]): MyValue = {
    env(v.i)
  }

  private def zeroEval(m: Monoid): MyValue = m match {
    case _: SetMonoid => SetValue(Set())
    case _: ListMonoid => ListValue(List())
    case _: BagMonoid => ???
    case _: AndMonoid => BooleanValue(true)
    case _: OrMonoid => BooleanValue(false)
    case _: SumMonoid => IntValue(0)
    case _: MultiplyMonoid => IntValue(1)
    case _: MaxMonoid => IntValue(0) // TODO
  }

  private def consCollectionEval(m: CollectionMonoid): MyValue => MyValue = m match {
    case _: SetMonoid => (e: MyValue) => SetValue(Set(e))
    case _: ListMonoid => (e: MyValue) => ListValue(List(e))
    case _: BagMonoid => (e: MyValue) => ???
  }

  private def mergeBoolEval(m: BoolMonoid): (MyValue, MyValue) => MyValue = m match {
    case b: AndMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value && b2.value)
    }
    case b: OrMonoid => (v1: MyValue, v2: MyValue) => (v1, v2) match {
      case (b1: BooleanValue, b2: BooleanValue) => BooleanValue (b1.value || b2.value)
    }
  }

  private def mergeEval(m: CollectionMonoid): (MyValue, MyValue) => MyValue = m match {
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

  private def unaryOpEval(op: UnaryOperator): MyValue => MyValue = op match {
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

  private def binaryOpEval(op: BinaryOperator): (MyValue, MyValue) => MyValue = op match {
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
    case ZeroMonoid(m) => zeroEval(m)
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

  def zeroEval(m: Monoid): MyValue = m match {
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