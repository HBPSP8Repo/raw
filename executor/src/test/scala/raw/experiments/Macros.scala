package raw.experiments

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

case class Location(filename: String, line: Int, column: Int)

class Macros(val c: scala.reflect.macros.blackbox.Context) {
  import c.universe._

  def testImpl(name: c.Expr[String], age: c.Expr[Int]): c.Expr[String] = {
    val code = q""" "OK" """

    c.typecheck(code)

    println(show(code))
    println(showRaw(code))
    c.Expr[String](code)
  }

  def impl: c.Expr[Location] = {
    import c.universe._
    val pos = c.macroApplication.pos
    val clsLocation = c.mirror.staticModule("Location") // get symbol of "Location" object
    c.Expr(Apply(Ident(clsLocation), List(Literal(Constant(pos.source.path)), Literal(Constant(pos.line)), Literal(Constant(pos.column)))))
  }
}

object Macros {
  def test(name:String, age:Int):String = macro Macros.testImpl
  def currentLocation: Location = macro Macros.impl
}
