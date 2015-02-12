/*
package raw.executor.reference

abstract class DataSource {
  def next(): Option[List[Any]]
}

case class MemoryDataSource(listValue: List[Any]) extends DataSource {
  private var index: Int = 0
  private val L: List[Any] = listValue

  def next(): Option[List[Any]] = {
    val n = index
    index += 1
    try {
      Some(List(L(n)))
    } catch {
      case ex: IndexOutOfBoundsException => None
    }
  }
}
*/