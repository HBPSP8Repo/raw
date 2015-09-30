package raw
package calculus

class Canonizer extends Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def strategy = canonize

  lazy val canonize = reduce(ruleFlattenPreds)

  /** Put expressions in canonical form.
    */
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

}
