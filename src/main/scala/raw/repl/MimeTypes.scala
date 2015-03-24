package raw.util

sealed abstract class MimeType

case class TextCsv(separator: String) extends MimeType

case object ApplicationJson extends MimeType