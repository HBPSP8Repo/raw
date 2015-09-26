package raw
package calculus

import org.kiama.rewriting.Rewriter._

import scala.collection.immutable.Seq

class Optimizer(val analyzer: SemanticAnalyzer) extends Transformer {

  import Calculus._

  def strategy = optimize

  lazy val optimize = reduce(removeFilters + removeUselessReduce) <* reduce(doClassicGroupBy)

  /** Remove redundant filters
    */
  // TODO: Move this to the Simplifier?
  lazy val removeFilters = rule[Exp] {
    case Filter(child, BoolConst(true)) => child.e
  }

  def sameExp(p: Pattern, e: Exp): Boolean = (p, e) match {
    case (i1: PatternIdn, i2: IdnExp) => i1.idn.idn == i2.idn.idn
    case (p1: PatternProd, r: RecordCons) if p1.ps.length == r.atts.length =>
      r.atts.map(a => a.idn).zipWithIndex.forall{case (i, idx) => i == s"_${idx+1}"}
      val x: Seq[((Pattern, AttrCons), Int)] = p1.ps.zip(r.atts).zipWithIndex
      x.forall{case ((pi, a: AttrCons), idx) => a.idn == s"_${idx+1}" && sameExp(pi, a.e)}
    case _ => false
  }

  private def isCollectionMonoid(e: Exp, m: CollectionMonoid) =
    analyzer.tipe(e) match {
      case CollectionType(`m`, _) => true
      case _ => false
    }

  lazy val removeUselessReduce = rule[Exp] {
    case Reduce(m: CollectionMonoid, g, e) if sameExp(g.p, e) && isCollectionMonoid(g.e, m) => g.e
  }

  private def rewriteExp[T <: RawNode](n: T, remap:Map[Exp, Exp]): T = {
    rewrite(
      everywhere(rule[Exp] {
        case e: Exp if remap.contains(e) => remap(e)
      }))(n)
  }

  private def rewriteIdnNode[T <: RawNode](n: T, remap:Map[Idn, Idn]): T = {
    rewrite(
      everywhere(rule[IdnNode] {
        case e: IdnDef if remap.contains(e.idn) => IdnDef(remap(e.idn))
        case e: IdnUse if remap.contains(e.idn) => IdnUse(remap(e.idn))
      }))(n)
  }

  private def alphaEq(e1: Exp, e2: Exp, ids:Map[Idn, Idn]) = {
    val x1 = rewriteIdnNode(e1,ids)
    val x2 = rewriteIdnNode(e2,ids)
    logger.debug(s"$x1 == $x2")
    x1 == x2
  }

  // TODO if predicate is an And of Eq() (e1=e2 && f1=f2) which are all OK it should group by (e1, f1), we have queries like that
  // TODO what if BoolConst(true) is not a true?

  lazy val doClassicGroupBy = rule[Exp] {
    case x @ UnaryExp(ToSet(),
              r @ Reduce(mr,
                     pat @ Gen(PatternProd(Seq(PatternIdn(ri1),PatternIdn(ri2))), n @ Nest(m, Gen(PatternProd(Seq(PatternIdn(i1),PatternIdn(i2))),
                          oj @ OuterJoin(g1 @ Gen(PatternIdn(oi1), oe1), Gen(PatternIdn(oi2), oe2), BinaryExp(Eq(), e1, e2))), k, BoolConst(true), h)),
                     pr)) if alphaEq(oe1, oe2, Map(oi1.idn -> oi2.idn)) && alphaEq(e1, e2, Map(oi1.idn -> oi2.idn)) => {
      val newK  = e1
      val newKo = rewriteIdnNode(newK, Map(i1.idn -> ri1.idn))
      logger.debug(s"pr = $pr")
      def tempfix[T <: RawNode](n: T): T = {
        rewrite(
          manytd(rule[Exp] {
            case r @ RecordProj(IdnExp(IdnUse(ri1.idn)), "title") => r.e
          }))(n)
      }
//      val newPr = rewriteExp(pr)
      val newPr = tempfix(pr)
      logger.debug(s"newpr = $newPr")
      UnaryExp(ToSet(), Reduce(mr, Gen(pat.p, Nest(m, g1, newK, BoolConst(true), rewriteExp(h, Map(IdnExp(IdnUse(i2.idn)) -> IdnExp(IdnUse(i1.idn)))))),
                                                          newPr))
    }
  }

//  /** Eq-joins are hash joins
//    */
//  lazy val eqJoinToHashJoin = rule[Exp] {
//    case Join(left, right, p)
//  }

}
