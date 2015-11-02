package raw
package calculus

import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

import scala.collection.immutable.Seq

class Optimizer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import Calculus._

  def transform = optimize

  lazy val optimize = reduce(removeFilters + removeUselessReduce) <* repeat(dealWithNestOuterJoin)

  /** Remove redundant filters
    */
  lazy val removeFilters = rule[Exp] {
    case Filter(child, BoolConst(true)) => child.e
  }

  /** Remove a reduce as a set of a set, list as a list, etc.
   */
  private def patternExp(op: Pattern): Exp = op match {
    case p: PatternIdn => IdnExp(IdnUse(p.idn.idn))
    case p: PatternProd => RecordCons(p.ps.zipWithIndex.map{ case (x, i) => AttrCons(s"_${i+1}", patternExp(x))})
  }

  private def sameExp(p: Pattern, e: Exp) = e == patternExp(p)

  private def isCollectionMonoid(e: Exp, m: CollectionMonoid) =
    analyzer.tipe(e) match {
      case CollectionType(m1, _) if analyzer.monoidsCompatible(m, m1) => true
      case _ => false
    }

  lazy val removeUselessReduce = rule[Exp] {
    case r @ Reduce(m: CollectionMonoid, Gen(Some(p), c), e) if sameExp(p, e) && isCollectionMonoid(c, m) => c
  }

  private def alphaEq(e1: Exp, e2: Exp): Option[Map[Exp, Exp]] = {
    var remap: Map[Exp, Exp] = Map()
    def recurse(e1: Exp, e2: Exp): Boolean = {
      (e1, e2) match {
        case (x1: OuterJoin, x2: OuterJoin) => recurse(x1.p, x2.p) && recurse(x1.left.e, x2.left.e) && recurse(x1.right.e, x2.right.e)
        case (x1: Join, x2: Join) => recurse(x1.p, x2.p) && recurse(x1.left.e, x2.left.e) && recurse(x1.right.e, x2.right.e)
        case (x1: OuterUnnest, x2: OuterUnnest) => recurse(x1.pred, x2.pred) && recurse(x1.child.e, x2.child.e) && recurse(x1.path.e, x2.path.e)
        case (x1: Unnest, x2: Unnest) => recurse(x1.pred, x2.pred) && recurse(x1.child.e, x2.child.e) && recurse(x1.path.e, x2.path.e)
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
        case (x1: RegexConst, x2: RegexConst) => x1.value == x2.value
        case (x1: StringConst, x2: StringConst) => x1.value == x2.value
        case (x1 @ IdnExp(IdnUse(i1)), x2 @ IdnExp(IdnUse(i2))) => if (remap.contains(x2) && remap(x2) == x1) true else { remap = remap + (x2 -> x1) ; true }
        case (x1: ParseAs, x2: ParseAs) => x1.p == x2.p && recurse(x1.e, x2.e)
        case _ => false
      }
    }
    val r = recurse(e1, e2)
    logger.debug(s"${CalculusPrettyPrinter(e1)} ~ ${CalculusPrettyPrinter(e2)} => $r")
    if (r) Some(remap) else None
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

  // general idea
  // if there is a nest. Pick the expression being the key. Change it into a pattern if possible (it is possible only
  // if the key is a combination of recordcons and idnuse.
  // if not possible, give up.
  // if possible, figure out if there is an outer join somewhere below which output is a tuple and that pattern would be the left side, and
  // the two inputs would be alpha equivalent and the predicate can be changed into a equipred.
  // if yes, rewrite the tree by:
  // - replacing the outer join by its right side
  // - replacing the nest by a nest2 with the key being the equality
  // try to change the outer-join pred into an equality between two exps.
  // ensure the exps are "the same" modulo some exp rewrite. Compute the map of that rewrite.
  // figure out where both are coming from and check they are coming from alphaEq collections.

//  def alphaEq2(e1: Exp, e2: Exp): Boolean = (e1, e2) match {
//    case (n: Nest2, _) => alphaEq2(n.child.e, e2)
//    case _ => alphaEq(e1, e2).isDefined
//
//  }

  def alphaEqChild(n: Exp, x: Exp): Boolean = {
    alphaEq(n, x).isDefined || (n match {
      case n2: Nest2 => alphaEqChild(n2.child.e, x)
      case _ => false
    })
  }

  def relatedOuterJoin(k: Exp, n: Exp): Option[Gen] = {
    logger.debug(s"relatedOuterJoin(${CalculusPrettyPrinter(k)}, ${CalculusPrettyPrinter(n)})")
    n match {
      case OuterJoin(g@Gen(p, o: OuterJoin), _, _) if patternExp(o.left.p.get) == k && alphaEq(o.left.e, o.right.e).isDefined && makeEquiPred(o.p).isDefined => Some(g)
      case Nest2(_, g@Gen(p, o: OuterJoin), _, _, _) if patternExp(o.left.p.get) == k && alphaEq(o.left.e, o.right.e).isDefined && makeEquiPred(o.p).isDefined => Some(g)
      //    case Nest(_, g@Gen(p, o: OuterJoin), _, _, _) if patternExp(o.left.p) == k && alphaEq(o.left.e, o.right.e).isDefined && makeEquiPred(o.p).isDefined => Some(g)
      //    case Nest(_, g@Gen(p, OuterJoin(Gen(lp, n: Nest2), Gen(rp, x), op)), _, _, _) if patternExp(lp) == k && alphaEqChild(n, x) && makeEquiPred(op).isDefined => Some(g)
      case Nest(_, g@Gen(p, OuterJoin(Gen(lp, n2), Gen(rp, x), op)), _, _, _) if patternExp(lp.get) == k && alphaEqChild(n2, x) && makeEquiPred(op).isDefined => Some(g)
      case o: OuterJoin => relatedOuterJoin(k, o.left.e)
      case n: Nest2 => relatedOuterJoin(k, n.child.e)
      case n: Nest => relatedOuterJoin(k, n.child.e)
      case _ => None
    }
  }

  def dealF: PartialFunction[Exp, Option[Exp]] = {
    case tree => {
      // we try all nests which don't have a predicate TODO (can we deal with that predicate?)
      val collectNests = collect[Seq, (Gen, Option[Gen])] {
//        case g@Gen(_, n: Nest) if n.p == BoolConst(true) => (g, relatedOuterJoin(n.k, n))
        case g@Gen(_, n: Nest) if n.p == BoolConst(true) => (g, relatedOuterJoin(n.k, n))
      }
      val nests = collectNests(tree)
      logger.debug(s"found ${nests.length} nests: $nests")
      // find one which seems to be over an outer join we can remove
      val optNest = nests.collectFirst { case (g, og) if og.isDefined => (g, og.get) }
      if (optNest.isEmpty)
        None
      else {
        // we have a nest
        val (nestG, ojG) = optNest.get
        val nest = nestG.e.asInstanceOf[Nest]
        val nestLeftOutput = nestG.p match {
          case Some(p: PatternProd) if p.ps.length == 2 => p.ps.head
        }
        // nestG is a Gen over an OuterJoin
        // jp1, jp2 are the patterns corresponding to the left and right
        // jexp is the exp which is going to replace the outer join
        // jpred is the join predicate to be changed in a key
        val (jp1, jp2, jexp, jpred) = ojG.e match {
          case OuterJoin(g1@Gen(Some(p1), e), g2@Gen(Some(p2), _), jpred) => (p1, p2, e, jpred)
        }
        // the key to be put in nest2
        val eqPred = makeEquiPred(jpred).get
        val nest2Key = eqPred match {
          case BinaryExp(Eq(), e1, e2) => e1
        }

        // we don't deal with eqPreds which are too complicated (more than one IdnUse on each side)
        val M = eqPred match {
          case BinaryExp(Eq(), e1, e2) =>
            val m = alphaEq(e1, e2)
            if (m.isEmpty) {
              logger.debug(s"could not find alphaEq(${CalculusPrettyPrinter(e1)}, ${e2})")
            }
            m
        }

        if (M.isDefined) {
          val m = M.get
          if (m.keys.size == 1 && m.values.size == 1) {

            val vDropped = m.keys.head
            val vReplacing = m.values.head

            val fixOJ = rule[RawNode] {
              case oj: OuterJoin if oj eq ojG.e => jexp
              case p: Pattern if p == ojG.p.get => jp1
              case e: RecordCons if e == patternExp(ojG.p.get) => patternExp(jp1)
              case e: Exp if e == vDropped => vReplacing
            }

            val nest2 = rewrite(alltd(fixOJ))(Nest2(nest.m, nest.child, nest2Key, nest.p, nest.e))
            logger.debug(s"will replace\n${CalculusPrettyPrinter(nestG)}\nby\n${CalculusPrettyPrinter(nest2)}")
            logger.debug(s"will rewrite ${CalculusPrettyPrinter(nestLeftOutput)} by ${CalculusPrettyPrinter(nest.child.p.get)}")

            val fixP =
              rule[Pattern] {
                case p: Pattern if p == nestLeftOutput => nest2.child.p.get
              }

            val fixN =
              rule[Exp] {
                case n: Nest if n eq nest => nest2
              }

            val newTree: Exp = rewrite(alltd(fixN <+ fixP))(tree)
            logger.debug(s"returned\n${CalculusPrettyPrinter(newTree)}")
            Some(newTree)
          } else {
            logger.debug(s"could not process m: (${m.toString})")
            None
          }
        } else {
          None
        }
      }
    }
  }

  val dealWithNestOuterJoin: Strategy = strategy(dealF)



}
