package raw
package util

import org.json4s.JsonAST._

case class JSONParserError(err: String) extends RawException(err)

object JSONParser {

  def apply(t: Type, path: String): List[Any] = {
    val content = scala.io.Source.fromFile(path)
    val json = org.json4s.native.JsonMethods.parse(content.mkString)

    def convert(t: Type, item: JValue): Any = (t, item) match {
      case (IntType(), JInt(i)) => i.toInt
      case (FloatType(), JDouble(d)) => d.toFloat
      case (BoolType(), JBool(b)) => b
      case (StringType(), JString(s)) => s
      case (RecordType(atts, _), JObject(l)) => {
        val jMap: Map[String, JValue] = l.map { jfield: JField => (jfield._1, jfield._2)}.toMap
        val tMap: Map[String, Type] = atts.map({ aType: AttrType => (aType.idn, aType.tipe)}).toMap
        val vMap: Map[String, Any] = jMap.map({ j => (j._1, convert(tMap(j._1), j._2))})
        vMap
      }
      case (ListType(innerType), JArray(arr)) => arr.map({ jv => convert(innerType, jv)})
      case _ => throw JSONParserError(s"Unexpected type: $t")
    }

    convert(t, json) match {
      case l: List[_] => l
      case _          => throw JSONParserError(s"Unexpected return type: $t")
    }
  }

}
