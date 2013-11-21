package raw

abstract class RawException(err: String) extends RuntimeException("[RawException] " + err)

case class RawInternalException(err: String) extends RuntimeException("[RawInternalException] " + err) {
  override def toString = "[RawInternalException] " + err
}