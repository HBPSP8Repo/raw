package raw

case class RawPosition(line: Int, column: Int)

case class RawParserPosition(begin: RawPosition, end: RawPosition)
