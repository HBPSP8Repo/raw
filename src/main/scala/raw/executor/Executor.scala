package raw.executor

import raw.algebra.Algebra.OperatorNode

/**
 * Created by gaidioz on 1/14/15.
 */

sealed trait MyValue

case class IntValue(value: Int) extends MyValue

case class FloatValue(value: Float) extends MyValue

case class BooleanValue(value: Boolean) extends MyValue

case class StringValue(value: String) extends MyValue

case class RecordValue(value: Map[String, MyValue]) extends MyValue

sealed abstract class CollectionValue() extends MyValue

case class ListValue(value: List[MyValue]) extends CollectionValue

case class SetValue(value: Set[MyValue]) extends CollectionValue

/*
def toString(value: MyValue): String = value match {
  case IntValue(i) => i
  case FloatValue(f) =>
}
*/

abstract class DataSource {
  def next(): Option[List[MyValue]]
}

case class MemoryDataSource(listValue: List[MyValue]) extends DataSource {
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

abstract class Executor(classes: Map[String, List[MyValue]]) {
  def execute(operatorNode: OperatorNode): List[MyValue]
}
