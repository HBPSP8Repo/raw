package raw

abstract class RawException extends RuntimeException

//TODO: Remove
case class RawInternalException(err: String) extends RawException