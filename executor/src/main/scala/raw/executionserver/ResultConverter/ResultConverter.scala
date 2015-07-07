package raw.executionserver

import java.io.StringWriter
import java.lang.reflect.Field

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.ImmutableMultiset

import scala.collection.JavaConversions

trait ResultConverter {
  val mapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    om.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    om.setSerializationInclusion(Include.NON_EMPTY)
    om
  }

  def convertToJson(res: Any): String = {
    val value = convertToCollection(res)
    println("Value: " + value)
    val sw = new StringWriter()
    mapper.writeValue(sw, value)
    sw.toString
  }

  private[this] def convertToCollection(res: Any): Any = {
    res match {
      case imSet: ImmutableMultiset[_] =>
        val resultTyped: List[Any] = toScalaList(imSet)
        resultTyped.map(convertToCollection(_))
      case set: Set[_] =>
        set.map(convertToCollection(_))
      case list: List[_] =>
        list.map(convertToCollection(_))
      case m: Map[_, _] =>
        m.map({ case (k, v) => s"$k: ${convertToCollection(v)}" })
      case p: Product =>
        val fields: Array[Field] = p.getClass.getDeclaredFields
        fields.map(f => {
          f.setAccessible(true)
          f.getName -> convertToCollection(f.get(p))
        }
        ).toMap
      case _ =>
        res.toString
    }
  }

  def convertToString(res: Any): String = {
    res match {
      case imSet: ImmutableMultiset[_] =>
        val resultTyped = toScalaList(imSet)
        resultsToString(resultTyped)
      case set: Set[_] =>
        resultsToString(set)
      case list: List[_] =>
        resultsToString(list)
      case _ => res.toString
    }
  }

  def toScalaList[T](s: ImmutableMultiset[T]) = {
    JavaConversions.asScalaIterator[T](s.iterator).toList
  }

  def resultsToString[T](s: Set[T]): String = {
    resultsToString(s.toList)
  }

  def resultsToString[T](l: List[T]): String = {
    //    l.map(valueToString(_)).sorted.mkString("\n")
    l.map(valueToString(_)).sorted.mkString("\n")
  }

  def valueToString[T](value: Any): String = {
    value match {
      //      case a: Author => valueToString(List(s"name: ${a.name}", s"title: ${a.title}", s"year: ${a.year}"))
      //      case p: Publication => valueToString(List(
      //        s"title: ${p.title}",
      //        s"authors: ${valueToString(p.authors)}",
      //        s"affiliations: ${valueToString(p.affiliations)}",
      //        s"controlledterms: ${valueToString(p.controlledterms)}"))
      case l: List[_] => l.map(valueToString(_)).sorted.mkString("[", ", ", "]")
      case s: Set[_] => s.map(valueToString(_)).toList.sorted.mkString("[", ", ", "]")
      case m: Map[_, _] => m.map({ case (k, v) => s"$k: ${valueToString(v)}" }).toList.sorted.mkString("[", ", ", "]")
      case p: Product => valueToString(caseClassToMapOfStrings(p))
      case _ => value.toString
    }
  }

  def convertExpected(expected: String): String = {
    expected.replaceAll("""\n\s*""", "\n").trim
  }

  // WARN: There are many classes that implement Product, like List. If this method is called with something other
  // than a case class, it will likely fail.
  def caseClassToMapOfStrings(p: Product): Any = {
    val fields: Array[Field] = p.getClass.getDeclaredFields
    fields.map(f => {
      f.setAccessible(true)
      f.getName -> valueToString(f.get(p))
    }
    ).toMap
  }
}