package raw.experiments

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros._

class WhiteMacrosInferStructuralType(val c: scala.reflect.macros.whitebox.Context) {
  import c.universe._

  def typedMacroImpl[T : c.WeakTypeTag](t:c.Expr[T]):c.Expr[Unit] = {
    println("Argument: " + t.getClass + " " + t.staticType)

    val code: Tree = q""" println("Received argument: " + $t) """
    c.Expr[Unit](code)
  }

  def createCaseImpl(): c.Expr[Product] = {
    val code: Tree = q""" case class Per(name:String); new Per("Joe") {}"""
    c.Expr(code)
  }
}

object WhiteMacrosInferStructuralType {
  def typedMacro[T](t:T):Unit = macro WhiteMacrosInferStructuralType.typedMacroImpl[T]
  def createCase(): Product = macro WhiteMacrosInferStructuralType.createCaseImpl
}

class queryResult extends StaticAnnotation {
  def queryResult(annottees: Any*):Any = macro QueryGenerator.queryResult_impl
}

class QueryGenerator(val c: scala.reflect.macros.whitebox.Context) {
  import c.universe._
  def queryResult_impl(annottees: c.Expr[Any]*):c.Expr[Any] = {
    println("Annotteees: " + annottees)
    val code = q""" object Results { case class Person(name:String) } """
    println("Generating code: " + code)
    c.Expr[Any](code)
  }
}
