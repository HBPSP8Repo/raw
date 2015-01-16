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
  def iterator = value.iterator
}

case class SetValue(value: Set[MyValue]) extends CollectionValue {
  def iterator = value.iterator
}
//case class BagValue(value: MultiSet[MyValue]) extends CollectionValue

class Blocks(elements: List[ListValue]) extends Iterable[List[MyValue]] {

  def content = elements

  class TupleIterator(elements: List[ListValue]) extends Iterator[List[MyValue]] {
    var iterators = elements.map({ e: ListValue => e.iterator })
    def next = iterators.map({i: Iterator[MyValue] => i.next})
    def hasNext = iterators.map({ i: Iterator[MyValue] => i.hasNext }).forall({_ == true})
  }


  def iterator = new TupleIterator(elements)
  override def equals(b: Any): Boolean = b match {
    case b: Blocks => elements == b.content
  }
}


abstract class Executor(classes: Map[String, ListValue]) {
  def execute(algebraNode: AlgebraNode): Blocks
}
