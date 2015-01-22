package raw.executor.reference

abstract class DataSource {
  def next(): Option[List[Value]]
}

case class MemoryDataSource(listValue: List[Value]) extends DataSource {
  private var index: Int = 0
  private val L: List[Value] = listValue

  def next(): Option[List[Value]] = {
    val n = index
    index += 1
    try {
      Some(List(L(n)))
    } catch {
      case ex: IndexOutOfBoundsException => None
    }
  }
}
