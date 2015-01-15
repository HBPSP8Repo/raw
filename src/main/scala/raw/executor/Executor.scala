package raw.executor

import raw.algebra.AlgebraNode

/**
 * Created by gaidioz on 1/14/15.
 */

sealed trait MyValue
case class IntValue(value: Int) extends MyValue
case class FloatValue(value: Float) extends MyValue
case class BooleanValue(value: Boolean) extends MyValue
case class StringValue(value: String) extends MyValue
case class RecordValue(value: Map[String, MyValue]) extends MyValue
sealed abstract class CollectionValue() extends MyValue with Iterable[MyValue]
case class ListValue(value: List[MyValue]) extends CollectionValue {
  override val iterator = value.iterator
}
case class SetValue(value: Set[MyValue]) extends CollectionValue {
  override val iterator = value.iterator
}
//case class BagValue(value: MultiSet[MyValue]) extends CollectionValue

abstract class Executor(classes: Map[String, CollectionValue]) {
  def execute(algebraNode: AlgebraNode): List[CollectionValue]
}
