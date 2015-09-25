package raw
package calculus

import org.kiama.rewriting.Rewriter._

trait Optimizer extends Unnester {

  import Calculus._

  override def strategy = attempt(super.strategy) <* optimize

  lazy val optimize = reduce(removeFilters)

  /** Remove redundant filters
    */
  lazy val removeFilters = rule[Exp] {
    case Filter(child, BoolConst(true)) => child.e
  }

//  /** Eq-joins are hash joins
//    */
//  lazy val eqJoinToHashJoin = rule[Exp] {
//    case Join(left, right, p)
//  }

}

object Optimizer {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Simplifier(tree, world)
    val optimizer = new Optimizer {}
    rewriteTree(optimizer.strategy)(t1)
  }
}
