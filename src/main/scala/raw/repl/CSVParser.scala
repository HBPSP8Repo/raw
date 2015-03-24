package raw
package util

case class CSVParserError(err: String) extends RawException(err)

object CSVParser {

  def apply[T](path: String, parse: (List[String] => T), delim: String = ","): List[T] = {
    val content = scala.io.Source.fromFile(path)
    content.getLines().map{ case line => line.split(delim).toList }.map(parse).toList
  }

}
//
//object CSVParser {
//
//  def apply(path: String, tipe: Type, delim: String = ","): List[Any] = {
//    val content = scala.io.Source.fromFile(path)
//
//    //    def parse(t: Type, item: String): Any = t match {
//    //      case IntType()    => item.toInt
//    //      case FloatType()  => item.toFloat
//    //      case BoolType()   => item.toBoolean
//    //      case StringType() => item
//    //      case _            => throw CSVParserError(s"Unexpected type: $t")
//    //    }
//
//    content.getLines().map{ case line => line.split(delim).toList }.map(parse)
//    //
//    //    t match {
//    //      case ListType(RecordType(atts)) => {
//    //        val f = (l: String) => atts.zip(l.split(delim)).map { case (a, item) => (a.idn, parse(a.tipe, item))}.toMap
//    //        // TODO: This is materializing the whole file in memory!
//    //        content.getLines().toList.map(f)
//    //      }
//    //      case _                => throw CSVParserError(s"Unexpected return type: $t")
//    //    }
//  }
//
//}
