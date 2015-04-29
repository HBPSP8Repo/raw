package raw.experiments

import raw._
import raw.csv.Student


import scala.language.experimental.macros

class RawMacros(val c: scala.reflect.macros.blackbox.Context) {

  import c.universe._

  def queryImpl(query: c.Expr[String], student: c.Expr[Student]): c.Expr[Any] = {
    println(query)
    println(query.mirror)
    println(query.actualType)
    println(query.staticType)

    println("Tree: " + query.tree)

    println(showRaw(student.tree))
    println(showRaw(query.tree))

    //    val tree = c.parse("""case class Person(str:String);val p = new Person("OK");println(p) """)
    val tree =
      q"""
         case class Person(str:String, name:Student)
         val p = new Person("OK", s)
         println(p)
         p
        """

//    tree.`
    println("Code: " + showCode(tree))
    c.Expr[String](tree)
  }
}

object RawMacros {
  def query(query: String, student: Student): Any = macro RawMacros.queryImpl
}
