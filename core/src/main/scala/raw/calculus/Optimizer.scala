package raw
package calculus

import org.kiama.rewriting.Rewriter._

import scala.collection.immutable.Seq

class Optimizer(val analyzer: SemanticAnalyzer) extends Transformer {

  import Calculus._

  def strategy = optimize

  lazy val optimize = reduce(removeFilters + removeUselessReduce) <* reduce(makeMultiNest)

  /** Remove redundant filters
    */
  lazy val removeFilters = rule[Exp] {
    case Filter(child, BoolConst(true)) => child.e
  }

  /** Remove redundant reduce nodes, e.g.
    * `to_set(reduce(bag, ($0, $1) <- nest(bag, $0 <- authors, $0.title, true, $0.year), (_1: $0, _2: $1)))`
    *   becomes
    * `to_set(nest(bag, $0 <- authors, $0.title, true, $0.year))`
    */

  private def sameExp(p: Pattern, e: Exp): Boolean = (p, e) match {
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
    case r @ Reduce(m: CollectionMonoid, g, e) if sameExp(g.p, e) && isCollectionMonoid(g.e, m) => g.e
  }

  /**
   */

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

//  private def alphaEq(e1: Exp, e2: Exp, ids:Map[Idn, Idn]) = {
//    val x1 = rewriteIdnNode(e1,ids)
//    val x2 = rewriteIdnNode(e2,ids)
//    logger.debug(s"$x1 == $x2")
//    x1 == x2
//  }

  // TODO if predicate is an And of Eq() (e1=e2 && f1=f2) which are all OK it should group by (e1, f1), we have queries like that
  // TODO what if BoolConst(true) is not a true?

//  lazy val doClassicGroupBy = rule[Exp] {
//    case x @ UnaryExp(ToSet(),
//              r @ Reduce(mr,
//                     pat @ Gen(PatternProd(Seq(PatternIdn(ri1),PatternIdn(ri2))), n @ Nest(m, Gen(PatternProd(Seq(PatternIdn(i1),PatternIdn(i2))),
//                          oj @ OuterJoin(g1 @ Gen(PatternIdn(oi1), oe1), Gen(PatternIdn(oi2), oe2), BinaryExp(Eq(), e1, e2))), k, BoolConst(true), h)),
//                     pr)) if alphaEq(oe1, oe2, Map(oi1.idn -> oi2.idn)) && alphaEq(e1, e2, Map(oi1.idn -> oi2.idn)) => {
//      val newK  = e1
//      val newKo = rewriteIdnNode(newK, Map(i1.idn -> ri1.idn))
//      logger.debug(s"pr = $pr")
//      def tempfix[T <: RawNode](n: T): T = {
//        rewrite(
//          manytd(rule[Exp] {
//            case r @ RecordProj(IdnExp(IdnUse(ri1.idn)), "title") => r.e
//          }))(n)
//      }
////      val newPr = rewriteExp(pr)
//      val newPr = tempfix(pr)
//      logger.debug(s"newpr = $newPr")
//      UnaryExp(ToSet(), Reduce(mr, Gen(pat.p, Nest(m, g1, newK, BoolConst(true), rewriteExp(h, Map(IdnExp(IdnUse(i2.idn)) -> IdnExp(IdnUse(i1.idn)))))),
//                                                          newPr))
//    }
//  }
  
  private def alphaEq(e1: Exp, e2: Exp): Boolean = {
    var remap: Map[Idn, Idn] = Map()
    def recurse(e1: Exp, e2: Exp): Boolean = {
      (e1, e2) match {
        case (x1: OuterJoin, x2: OuterJoin) => recurse(x1.p, x2.p) && recurse(x1.left.e, x2.left.e) && recurse(x1.right.e, x2.right.e)
        case (x1: Join, x2: Join) => recurse(x1.p, x2.p) && recurse(x1.left.e, x2.left.e) && recurse(x1.right.e, x2.right.e)
        case (x1: OuterUnnest, x2: OuterUnnest) => recurse(x1.pred, x2.pred) && recurse(x1.child.e, x2.child.e) && recurse(x1.path.e, x2.path.e)
        case (x1: Unnest, x2: Unnest) => recurse(x1.pred, x2.pred) && recurse(x1.child.e, x2.child.e) && recurse(x1.path.e, x2.path.e)
        case (x1: MultiNest, x2: MultiNest) => x1.params.zip(x2.params).forall{case (p1, p2) => p1.m == p2.m && recurse(p1.k, p2.k) && recurse(p1.e, p2.e) && recurse(p1.p, p2.p)} && recurse(x1.child.e, x2.child.e)
        case (x1: Nest, x2: Nest) => x1.m == x2.m && recurse(x1.k, x2.k) && recurse(x1.e, x2.e) && recurse(x1.p, x2.p) && recurse(x1.child.e, x2.child.e)
        case (x1: Reduce, x2: Reduce) => x1.m == x2.m && recurse(x1.child.e, x2.child.e)
        case (x1: Filter, x2: Filter) => recurse(x1.p, x2.p) && recurse(x1.child.e, x2.child.e)
        case (x1: MergeMonoid, x2: MergeMonoid) => x1.m == x2.m && recurse(x1.e1, x2.e1) && recurse(x1.e2, x1.e2)
        case (x1: BinaryExp, x2: BinaryExp) => x1.op == x2.op && recurse(x1.e1, x2.e1) && recurse(x1.e2, x1.e2)
        case (x1: UnaryExp, x2: UnaryExp) => x1.op == x2.op && recurse(x1.e, x2.e)
        case (x1: RecordProj, x2: RecordProj) => x1.idn == x2.idn && recurse(x1.e, x2.e)
        case (x1: RecordCons, x2: RecordCons) => x1.atts.length == x2.atts.length && x1.atts.zip(x2.atts).forall{case (a1, a2) => a1.idn == a2.idn && recurse(a1.e, a2.e)}
        case (x1: IntConst, x2: IntConst) => x1.value == x2.value
        case (x1: BoolConst, x2: BoolConst) => x1.value == x2.value
        case (x1: FloatConst, x2: FloatConst) => x1.value == x2.value
        case (x1: StringConst, x2: StringConst) => x1.value == x2.value
        case (x1 @ IdnExp(IdnUse(i1)), x2 @ IdnExp(IdnUse(i2))) => if (remap.contains(i2)) remap(i2) == i1 else { remap = remap + (i2 -> i1) ; true }
        case _ => false
      }
    }
    val r = recurse(e1, e2)
    logger.debug(s"${CalculusPrettyPrinter(e1)} ~ ${CalculusPrettyPrinter(e2)} => $r")
    r
  }

  private def makeEquiPred(p: Exp): Option[BinaryExp] = {
    val r = p match {
      case MergeMonoid(AndMonoid(), e1, e2) => (makeEquiPred(e1), makeEquiPred(e2)) match {
        case (Some(p1), Some(p2)) => (p1, p2) match {
          case (x1@BinaryExp(Eq(), e11, e12), x2@BinaryExp(Eq(), e21, e22)) =>
            Some(BinaryExp(Eq(), RecordCons(Seq(AttrCons("_1", e11), AttrCons("_2", e21))), RecordCons(Seq(AttrCons("_1", e12), AttrCons("_2", e22)))))
          case (x1, x2) => Some(BinaryExp(Eq(), x1, x2))
        }
      }
      case b @ BinaryExp(Eq(), e1, e2) => Some(b)
      case _ => None
    }
    if (r.isDefined) logger.debug(s"makeEquiPred(${CalculusPrettyPrinter(p)}) => ${CalculusPrettyPrinter(r.get)}") else logger.debug(s"makeEquiPred(${CalculusPrettyPrinter(p)}) => None")
    r
  }

  private def idn_remap(p1: Pattern, p2: Pattern): Map[Idn,Idn] = {
    def recurse(p1: Pattern, p2: Pattern): Seq[(Idn, Idn)] = (p1, p2) match {
      case (i1: PatternIdn, i2: PatternIdn) => Seq((i1.idn.idn, i2.idn.idn))
      case (x1: PatternProd, i2: PatternIdn) => x1.ps.flatMap{p => idn_remap(p, i2)}
      case (x1: PatternProd, x2: PatternProd) => x1.ps.zip(x2.ps).flatMap{ case (v1, v2) => recurse(v1, v2) }
    }
    val r = recurse(p1, p2).toMap
    logger.debug(s"idn_remap(${CalculusPrettyPrinter(p1)}, ${CalculusPrettyPrinter(p2)}) = $r}")
    r
  }


//  lazy val makeNest2 = rule[Exp] {
//    case n@Nest(m, g@Gen(PatternProd(Seq(left, right)), _), k, pred_nest, h) if sameExp(left, k) => Nest2(m, g, k, pred_nest, h)
//  }

//  lazy val collapseNest2s = rule[Exp] {
//
//    case n1 @ Nest(m1, g1 @ Gen(PatternProd(Seq(left1, right1)), n2 @ Nest2(m2, g2 @ Gen(PatternProd(Seq(left2, right2)), ojoin @ OuterJoin(og1, og2, join_pred)), k2, pred_nest2, h2), k1, pred_nest1, h1)
//      if selfJoinRoot(n2).isDefined && makeEquiPred(join_pred).isDefined && sameExp(left2, k1) => {
//      val newK = makeEquiPred(join_pred).get match {
//        case BinaryExp(_, e1, e2) if true || alphaEq(e1, e2) => e1
//      }
//      val newIdn = selfJoinRoot(ojoin).get.p match {
//        case p: PatternIdn => p
//        case PatternProd(_:: (p: PatternIdn)) => p
//      }
//      Nest2(m, g1, newK, pred_nest, rewriteIdnNode(h, idn_remap(right, newIdn)))
//    }
//
//  }

  lazy val makeMultiNest = rule[Exp] {

//    case n @ Nest(m, g @ Gen(PatternProd(Seq(left, right)), ojoin @ OuterJoin(g1, g2, join_pred)), k, pred_nest, h)
//      if selfJoinRoot(ojoin).isDefined && makeEquiPred(join_pred).isDefined && sameExp(left, k) => {
//      val newK = makeEquiPred(join_pred).get match {
//        case BinaryExp(_, e1, e2) if true || alphaEq(e1, e2) => e1
//      }
//      val newIdn = selfJoinRoot(ojoin).get.p match {
//        case p: PatternIdn => p
//        case PatternProd(_:: (p: PatternIdn)) => p
//      }
//      MultiNest(g1, Seq(NestParams(m, newK, pred_nest, rewriteIdnNode(h, idn_remap(right, newIdn)))))
//    }


//    case n @ Nest(m, g @ Gen(PatternProd(Seq(left @ PatternProd(Seq(l, r)), right)), mnest @ MultiNest(gm, params)), k, pred_nest, h)
//      if sameExp(l, k) => {
//      MultiNest(gm, params :+ NestParams(m, rewriteIdnNode(k, idn_remap(g.p, l)), pred_nest, rewriteIdnNode(h, idn_remap(g.p, l))))
//    }

    case n @ Nest(m, g @ Gen(p @ PatternProd(Seq(left @ PatternProd(Seq(l, r)), right)), mnest @ MultiNest(gm @ Gen(PatternProd(Seq(left2, right2)), ojoin @ OuterJoin(g1, g2, join_pred)), params)), k, pred_nest, h)
      if selfJoinRoot(ojoin).isDefined && makeEquiPred(join_pred).isDefined && sameExp(l, k) => {
      val newK = makeEquiPred(join_pred).get match {
        case BinaryExp(_, e1, e2) if true || alphaEq(e1, e2) => e1
      }
      val leftMost = selfJoinRoot(ojoin).get.p
      MultiNest(Gen(PatternProd(Seq(l, right)), MultiNest(Gen(left2, g1.e), params)),
                Seq(NestParams(m, rewriteIdnNode(newK, idn_remap(r, leftMost)), pred_nest, rewriteIdnNode(h, idn_remap(r, leftMost)))))
      //MultiNest(Gen(l, g1.e), params :+ NestParams(m, rewriteIdnNode(newK, idn_remap(r, l)), pred_nest, rewriteIdnNode(h, idn_remap(r, l))))
    }

    case n @ Nest(m, g @ Gen(p @ PatternProd(Seq(left @ PatternProd(Seq(l, r)), right)), ojoin @ OuterJoin(g1 @ Gen(left2, mnest: MultiNest), g2, join_pred)), k, pred_nest, h)
      if selfJoinRoot(ojoin).isDefined && makeEquiPred(join_pred).isDefined && sameExp(left, k) => {
      val newK = makeEquiPred(join_pred).get match {
        case BinaryExp(_, e1, e2) if true || alphaEq(e1, e2) => e1
      }
      val leftMost = selfJoinRoot(ojoin).get.p
      MultiNest(Gen(PatternProd(Seq(l, r)), mnest),
        Seq(NestParams(m, rewriteIdnNode(newK, idn_remap(right, leftMost)), pred_nest, rewriteIdnNode(h, idn_remap(right, leftMost)))))
    }

    case n @ Nest(m, g @ Gen(PatternProd(Seq(left, right)), ojoin @ OuterJoin(g1, g2, join_pred)), k, pred_nest, h)
      if selfJoinRoot(ojoin).isDefined && makeEquiPred(join_pred).isDefined && sameExp(left, k) => {
      val newK = makeEquiPred(join_pred).get match {
        case BinaryExp(_, e1, e2) if true || alphaEq(e1, e2) => e1
      }

      val leftMost = selfJoinRoot(ojoin).get.p

      val map = idn_remap(right, leftMost)
      MultiNest(g1, Seq(NestParams(m, rewriteIdnNode(k, map), pred_nest, rewriteIdnNode(h, map))))
    }

  }

  def selfJoinRoot(e: Exp): Option[Gen] = {

    def recurse(e: Exp): Option[Gen] = {
      logger.debug(s"recurse:joinroot ${e}")
      val r = e match {
        case OuterJoin(g1, g2, p) if alphaEq(g1.e, g2.e) => Some(g1)
        case OuterJoin(Gen(_, i: OuterJoin), g2, p) => recurse(i) match {
          case Some(r) if alphaEq(r.e, g2.e) => Some(r)
          case _ => None
        }
        case OuterJoin(Gen(_, i: MultiNest), g2, p) => recurse(i) match {
          case Some(r) if alphaEq(r.e, g2.e) => Some(r)
          case _ => None
        }
        case MultiNest(Gen(_, i: MultiNest), _) => recurse(i)
        case MultiNest(Gen(_, i: OuterJoin), _) => recurse(i)
        case MultiNest(g, _) => Some(g)
        case _ => None
      }
      r match {
        case Some(x) => logger.debug(s"selfJoinRoot(${CalculusPrettyPrinter(e)}) => ${CalculusPrettyPrinter(x)}")
        case None => logger.debug(s"selfJoinRoot(${CalculusPrettyPrinter(e)}) => None")
      }
      r
    }
    recurse(e)
  }

//  /** Eq-joins are hash joins
//    */
//  lazy val eqJoinToHashJoin = rule[Exp] {
//    case Join(left, right, p)
//  }

}
