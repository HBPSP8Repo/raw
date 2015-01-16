package raw.executor

import raw._
import raw.algebra.Algebra._

/**
 * Created by gaidioz on 1/14/15.
 */

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
    case Reduce(m, e, ps, child) => {
      def recurse: Option[List[MyValue]] = next(child) match {
        case r@Some(v) => if (evalPredicates(ps, v)) r else recurse
        case None => None
      }
      recurse
    }
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
  }

  def consCollectionEval(m: CollectionMonoid): MyValue => MyValue = m match {
    case _: SetMonoid => (e: MyValue) => SetValue(Set(e))
    case _: ListMonoid => (e: MyValue) => ListValue(List(e))
    case _: BagMonoid => (e: MyValue) => ???
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


