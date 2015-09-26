package raw
package calculus

import org.kiama.rewriting.Rewriter._

import scala.collection.immutable.Seq

trait Optimizer extends Unnester {

  import Calculus._

  override def strategy = attempt(super.strategy) <* optimize

    lazy val optimize = reduce(removeFilters + removeAndTrue + removeUselessReduce) <* reduce(doClassicGroupBy)

  /** Remove redundant filters
    */
  lazy val removeFilters = rule[Exp] {
    case Filter(child, BoolConst(true)) => child.e
  }

  lazy val removeAndTrue = rule[Exp] {
    case MergeMonoid(AndMonoid(), BoolConst(true), e) => e
    case MergeMonoid(AndMonoid(), e, BoolConst(true)) => e
  }

  def sameExp(p: Pattern, e: Exp): Boolean = (p, e) match {
    case (i1: PatternIdn, i2: IdnExp) => i1.idn.idn == i2.idn.idn
    case (p1: PatternProd, r: RecordCons) if p1.ps.length == r.atts.length =>
      r.atts.map(a => a.idn).zipWithIndex.forall{case (i, idx) => i == s"_${idx+1}"}
      val x: Seq[((Pattern, AttrCons), Int)] = p1.ps.zip(r.atts).zipWithIndex
      x.forall{case ((pi, a: AttrCons), idx) => a.idn == s"_${idx+1}" && sameExp(pi, a.e)}
    case _ => false
  }

  // TODO copy of the one in Simplifier (since they don't inherit?)
  def analyzer: SemanticAnalyzer
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

object Optimizer {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Simplifier(tree, world)
    val a = new SemanticAnalyzer(t1, world)
    val optimizer = new Optimizer {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(optimizer.strategy)(t1)
  }
}
