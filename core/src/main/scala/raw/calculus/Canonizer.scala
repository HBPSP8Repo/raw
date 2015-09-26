package raw
package calculus

class Canonizer extends Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  /**
   *
   * the canonizer should really split things up completely, at the root level.
   *  only idnexp or recordproj left
   *  and btw, what is the root of the record proj? should become an idnexp
   *
   *
   *
   * @return
   */

  def strategy = canonize

  lazy val canonize = reduce(ruleFlattenPreds + convertTosToExpBlock)

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

  /**
   *
   */

  private object MatchToConversion {
    def unapply(qs: Seq[Gen]) = splitWith[Gen, Gen](qs, { case g @ Gen(_, UnaryExp((_: ToBag | _: ToSet | _: ToList), _)) => g})
  }

  // TODO: MatchToConversion is useless since we just want to know that there is one, but not have it
  lazy val convertTosToExpBlock = rule[Exp] {
    case CanComp(m, gs @ MatchToConversion(q, g, s), ps, e) =>
      val toConvs = gs.collect { case g @ Gen(_, UnaryExp((_: ToBag | _: ToSet | _: ToList), _)) => g }
      val ms = toConvs.map { case t => (t, SymbolTable.next()) }.toMap

      ExpBlock(
        ms.map { case (g1, sym) => Bind(PatternIdn(IdnDef(sym.idn)), g1.e)}.to,
        CanComp(m, gs.map { case g1 if toConvs.contains(g1) => Gen(g1.p, IdnExp(IdnUse(ms(g1).idn))) case g1 => g1 }, ps, e)
      )
  }

  // TODO: If ToSet, create ExpBlock!!!

}
