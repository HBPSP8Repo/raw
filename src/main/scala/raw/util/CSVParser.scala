package raw
package util

case class CSVParserError(err: String) extends RawException(err)

object CSVParser {

  def apply(t: Type, path: String, delim: String = ","): List[Any] = {
    val content = scala.io.Source.fromFile(path)

    def parse(t: Type, item: String): Any = t match {
      case IntType()    => item.toInt
      case FloatType()  => item.toFloat
      case BoolType()   => item.toBoolean
      case StringType() => item
      case _            => throw CSVParserError(s"Unexpected type: $t")
    }

    t match {
      case ListType(RecordType(atts)) => {
        val f = (l: String) => atts.zip(l.split(delim)).map { case (a, item) => (a.idn, parse(a.tipe, item))}.toMap
        // TODO: This is materializing the whole file in memory!
        content.getLines().toList.map(f)
      }
      case _                => throw CSVParserError(s"Unexpected return type: $t")
    }
  }

}
