package raw
package calculus


trait Canonizer extends Normalizer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  override def strategy = attempt(super.strategy) <* canonize

  lazy val canonize = reduce(ruleFlattenPreds)

  lazy val ruleFlattenPreds = rule[Exp] {
    case Comp(m, qs, e) =>
      val gs = qs.collect { case g: Gen => g }
      val ps = qs.collect { case p: Exp => p }.flatMap(flattenPreds)
      CanComp(m, gs, ps, e)
  }

  private def flattenPreds(p: Exp): Seq[Exp] = p match {
    case MergeMonoid(_: AndMonoid, e1, e2) => flattenPreds(e1) ++ flattenPreds(e2)
    case _                                 => Seq(p)
  }

  // TODO: If ToSet, create ExpBlock!!!

}

object Canonizer {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree, world)
    val a = new SemanticAnalyzer(t1, world)
    val canonizer = new Canonizer {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(canonizer.strategy)(t1)
  }
}
