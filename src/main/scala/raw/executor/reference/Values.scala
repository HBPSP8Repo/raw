package raw.executor.reference

sealed trait Value // TODO: Use a trait or an abstract class?

case class NullValue() extends Value

case class IntValue(value: Int) extends Value

case class FloatValue(value: Float) extends Value

case class BooleanValue(value: Boolean) extends Value

case class StringValue(value: String) extends Value

case class RecordValue(value: Map[String, Value]) extends Value

sealed abstract class CollectionValue extends Value

case class ListValue(value: List[Value]) extends CollectionValue

case class SetValue(value: Set[Value]) extends CollectionValue
