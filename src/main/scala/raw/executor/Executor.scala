package raw.executor

import raw.DataLocation
import raw.logical.Algebra.OperatorNode
import raw.calculus.World

/**
 * Created by gaidioz on 1/14/15.
 */

sealed trait MyValue // TODO

case class IntValue(value: Int) extends MyValue

case class FloatValue(value: Float) extends MyValue

case class BooleanValue(value: Boolean) extends MyValue

case class StringValue(value: String) extends MyValue

case class RecordValue(value: Map[String, MyValue]) extends MyValue

sealed abstract class CollectionValue extends MyValue

case class ListValue(value: List[MyValue]) extends CollectionValue

case class SetValue(value: Set[MyValue]) extends CollectionValue

abstract class DataSource {
  def next(): Option[List[MyValue]]
}

case class MemoryDataSource(listValue: List[MyValue]) extends DataSource {
  private var index: Int = 0
  private val L: List[MyValue] = listValue

  def next(): Option[List[MyValue]] = {
    val n = index
    index += 1
    try {
      Some(List(L(n)))
    } catch {
      case ex: IndexOutOfBoundsException => None
    }
  }
}

abstract class Executor(schema: World, dataLocations: Map[String, DataLocation]) {
  //def execute(operatorNode: OperatorNode): ExecutorResult
  def execute(operatorNode: OperatorNode): MyValue
}

abstract class ExecutorResult {
  def iterator: AnyRef
}