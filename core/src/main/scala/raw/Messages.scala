package raw

sealed abstract class Message

case class ErrorMessage(n: RawNode, msg: String) extends Message

case object NoMessage extends Message
