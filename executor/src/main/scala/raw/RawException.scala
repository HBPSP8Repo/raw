package raw

abstract class RawException(message: String) extends RuntimeException {
  override def toString = message
}
