package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

/** Desugar expression blocks.
  */
object DesugarExpBlocks extends LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def apply(tree: Calculus): Calculus =
    rewriteTree(desugar)(tree)

  lazy val desugar = reduce(ruleExpBlocks + ruleEmptyExpBlock)

  /** De-sugar expression blocks by removing the binds one-at-a-time.
    */

  lazy val ruleExpBlocks = rule[ExpBlock] {
    case ExpBlock(Bind(IdnDef(x, _), u) :: rest, e) =>
      logger.debug("Applying desugar ruleExpBlocks")
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => deepclone(u)
      })
      val nrest = rewrite(strategy)(rest)
      val ne = rewrite(strategy)(e)
      ExpBlock(nrest, ne)
  }

  /** De-sugar expression blocks without bind statements.
    */

  lazy val ruleEmptyExpBlock = rule[Exp] {
    case ExpBlock(Nil, e) =>
      logger.debug("Applying desugar ruleEmptyExpBlock")
      e
  }

}
