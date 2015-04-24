package raw.experiments

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object BFS extends Traverser {
  override def traverse(tree: Tree): Unit = {
    println("At node: " + tree.getClass + ", tpe: " + tree.tpe + ", symbol: " + tree.symbol + ", Pos: " + tree.pos)


    tree match {
      case Apply(fn, e) => println("Apply: " + fn + ", " + e)
      case _ =>
    }
    super.traverse(tree)
  }
}

object MacrosClient {

  def showTree(tree: Tree) {
    BFS.traverse(tree)
    println(showRaw(tree))
    println(show(tree))
    println(showCode(tree))
  }

  def toolbox() = {
    val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
    val tree = tb.parse(""" { class C(s:String) { def name = "a" } } """)
    showTree(tree)

    // fills in the symbol and tpe fields in the tree node.
    val typedTree = tb.typecheck(tree)
    showTree(typedTree)
  }

  def quasiquotes() = {
    // Does not resolve relative names
    val tree2 = q""" { class C(s:String) { def name = "a" } } """
    println(showRaw(tree2))
    println(show(tree2))

    val tree3 = q"println";
    println(showRaw(tree3))
    println(show(tree3))
  }

  def reifi() {
    // Resolves relative to absolute names, eg, String  -> Predef.String
    val expr = reify({
      class C(s: String) {
        def name = "a"
      }
    })
    showTree(expr.tree)

    println("Actual type: " + expr.actualType + ", Static type: " + expr.staticType)
    val ex = reify( { val x = 1 } )
    showTree(ex.tree)
    println("Actual type: " + ex.actualType + ", Static type: " + ex.staticType)
  }

  def main(args: Array[String]) {
    //    print(Macros.test("Joe", 12))
//    reifi()
//    quasiquotes()
    toolbox()
  }
}
