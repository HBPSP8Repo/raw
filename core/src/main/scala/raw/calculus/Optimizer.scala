package raw
package calculus

import org.kiama.rewriting.Rewriter._

trait Optimizer extends Transformer {

  import Calculus._

  def strategy = optimize

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

}