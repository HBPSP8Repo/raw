package raw.experiments

import scala.language.experimental.macros

class WhiteMacrosInferStructuralType(val c: scala.reflect.macros.whitebox.Context) {
  import c.universe._

  def createCaseImpl(): c.Expr[Product] = {
    val code: Tree = q""" case class Per(name:String); new Per("Joe") """
    c.Expr(code)
  }
}

object WhiteMacrosInferStructuralType {
  def createCase(): Product = macro WhiteMacrosInferStructuralType.createCaseImpl
}
