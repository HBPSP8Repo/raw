package raw
package calculus

import org.kiama.attribution.Attribution

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
trait Unnester extends Attribution with Canonizer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._
  import Calculus._

  override def strategy = attempt(super.strategy) <* unnester

  private lazy val unnester =
    repeat(oncetd(ruleInnerPrimitiveReduceOnPredicate)) <*
    reduce(ruleScanFilter <+ ruleUnnest <+ ruleJoin <+ ruleReduceExpBlock <+ (ruleReduce + ruleNest))

  // TODO: When applying the last optimization of (for x <- Algebra) yield set (x.name, Algebra),
  // look at the monoid of 'for' and if it is a set, we can directly group by x.name w/o having to care
  // about value equality

  /** Scan + Filter
    * e.g.
    * `for (d <- departments; d.year > 1950; ...)`
    *   becomes
    * `for (d <- Filter(d <- departments; d.year > 1950); ...`
    */

  private object GenOverIdnExp {
    def unapply(qs: Seq[Gen]) = splitWith[Gen, Gen](qs, { case g @ Gen(_, _: IdnExp) => g})
  }

  private lazy val ruleScanFilter = rule[Exp] {
    case CanComp(m, GenOverIdnExp(r, g, u), ps, e) =>
      logger.debug(s"Applying ruleScanFilter")
      val (ps_g, ps_not_g) = ps.partition(variables(_) == idns(g.p))
      CanComp(m, r ++ Seq(Gen(deepclone(g.p), Filter(g, foldPreds(ps_g)))) ++ u, ps_not_g, e)
  }

  /** Unnest
    * e.g.
    * `for (d <- Filter(d <- departments, d.year > 1950); s <- d.students; s.age > 18; ...)`
    *   becomes
    * `for ((d, s) <- Unnest(d <- Filter(d <- departments, d.year > 1950), s <- d.students, s.age > 18); ...)`
    */

  private object GenOverRecordProj {
    def unapply(qs: Seq[Gen]) = splitWith[Gen, Gen](qs, { case g @ Gen(_, _: RecordProj) => g})
  }

  private lazy val idns: Pattern => Set[Idn] = attr {
    case PatternIdn(IdnDef(idn)) => Set(idn)
    case PatternProd(ps)         => ps.flatMap(idns).to
  }

  private lazy val ruleUnnest = rule[Exp] {
    case c @ CanComp(m, GenOverRecordProj(r, g, u), ps, e) =>
      logger.debug(s"Applying ruleUnnest")
      val (ps_g, ps_not_g) = ps.partition(variables(_) == idns(g.p))

      // Find the root of the record projections (e.g. we must retrieve 'd' from d.info.students as well as from d.students)
      def rootProj(e: Exp): IdnExp = e match {
        case RecordProj(idn: IdnExp, _) => idn
        case RecordProj(e1, _) => rootProj(e1)
      }

      val rootIdn = rootProj(g.e).idn.idn

      logger.debug(s"Just here with ${CalculusPrettyPrinter(c)}")
      // Find the generator where the identifier is defined
      val og = r.collectFirst { case g @ Gen(p, _) if idns(p).contains(rootIdn) => g }.head

      // Remove that generator from r
      val nr = r.filter { case n if n eq og => false case _ => true }

      CanComp(m, nr ++ Seq(Gen(PatternProd(Seq(deepclone(og.p), deepclone(g.p))), Unnest(og, g, foldPreds(ps_g)))) ++ u, ps_not_g, e)
  }

  /** Join
    * e.g.
    * `for (s <- students; p <- professors; s.age > p.age) ...`
    *   becomes
    * `for ((s, p) <- Join(s <- students; p <- professors; s.age > p.age)) ...`
    */

  private lazy val ruleJoin = rule[Exp] {
    case CanComp(m, g1 :: g2 :: rest, ps, e) =>
      logger.debug(s"Applying ruleJoin")

      val idns1 = idns(g1.p)
      val idns2 = idns(g2.p)

      val (ps_12, ps_not_12) = ps.partition { case p =>
        val vs = variables(p)
        vs.intersect(idns1).nonEmpty && vs.intersect(idns2).nonEmpty
      }
      CanComp(m, Seq(Gen(PatternProd(Seq(g1.p, g2.p)), Join(deepclone(g1), deepclone(g2), foldPreds(ps_12)))) ++ rest, ps_not_12, e)
  }

  /** Reduce
    * e.g.
    * `for (d <- departments; d.year > 1950) yield set d.name`
    *   becomes
    * `reduce(set, d <- departments, d.year > 1950, d.name)`
    *
    */
  private lazy val ruleReduce = rule[Exp] {
    case CanComp(m, g :: Nil, Nil, e) if nestedComp(e).isEmpty =>
      logger.debug(s"Applying ruleReduce")
      Reduce(m, g, e)
  }

  private lazy val nestedComp: Exp => Seq[CanComp] = attr {
    case e: Exp =>
      val collectComps = collect[Seq, CanComp] {
        case n: CanComp => n
      }
      collectComps(e)
  }

  /** Nest
    * e.g.
    * `for (d <- Filter(d <- departments, d.year > 1950)) yield set (d.name, for (x <- Filter(x <- departments, true); x.year = d.year) yield list x.name)`
    *   becomes
    * `for ((d, $42) <- Nest((d, x) <- OuterJoin(d <- Filter(d <- departments, d.year > 1950),
    *                                            x <- Filter(x <- departments, true),
    *                                            x.year = d.year),
    *                        d, list, x.name))
    *   yield set (d.name, $42)`
   */
  private lazy val ruleNest = rule[Exp] {
    case CanComp(m, g :: Nil, Nil, e) if nestedComp(e).nonEmpty =>
      logger.debug(s"Applying ruleNest")

      // Get the first inner comprehension
      val c1 = nestedComp(e).head

      // Rewrite the algebra of the inner comprehension to be outer
      val nc1 = rewrite(
        everywhere(rule[Exp] {
          case Unnest(child, path, pred) => OuterUnnest(child, path, pred)
          case Join(left, right, p) => OuterJoin(left, right, p)
        }))(c1)

      nc1 match {
        case CanComp(m1, g1 :: Nil, ps1, e1) =>
          // OuterJoin both "comprehensions"
          // The join predicates are the left-over predicates on the rhs
//          assert(!ps1.map(variables(_) == (idns(g.p) ++ idns(g1.p)).toSet).exists(!_))
          val j = OuterJoin(g, g1, foldPreds(ps1))

          // Create Nest node
          // Group-by the pattern of the 1st comprehension
          val n = Nest(m1, Gen(PatternProd(Seq(deepclone(g.p), deepclone(g1.p))), j), createRecord(g.p), e1)

          // TODO: Check with Ben: Is this still the same semantics as his "implementation/execution" of Nest? ...
          // TODO: Check with Ben: ... because if predicate changes for them all, I still want to have the notion of None
          //                       ... which I think I do actually

          // Create new CanComp
          val nsym = SymbolTable.next().idn
          val ne = rewrite(
            everywhere(rule[Exp] {
              case n if n eq c1 => IdnExp(IdnUse(nsym))
            }))(e)
          CanComp(m, Gen(PatternProd(Seq(deepclone(g.p), PatternIdn(IdnDef(nsym)))), n) :: Nil, Nil, ne)
      }
  }

  /**
   *
   */
//  private lazy val nestedReduce: Exp => Seq[Reduce] = attr {
//    case e: Exp =>
//      val collectReduces = collect[Seq, Reduce] {
//        case n: Reduce => n
//      }
//      collectReduces(e)
//  }

  // TODO: Make this NOT JUST FOR PRIMITIVE!!

  lazy val compIdns: CanComp => Set[Idn] = attr {
    case c => c.gs.flatMap { case g => idns(g.p) }.toSet
  }

  lazy val nestedIndependentComp: Exp => Seq[CanComp] = attr {
    case e =>
      logger.debug(s"Here with e ${CalculusPrettyPrinter(e)}")
      val c = nestedComp(e)
      val sources = analyzer.world.sources.keySet
      if (c.size > 0) {
        logger.debug(s"Here with x ${CalculusPrettyPrinter(c.head)}")
        logger.debug(s"vars ${variables(c.head)}")
        logger.debug(s"Here ${compIdns(c.head)}")
        logger.debug(s"vars ${variables(c.head).filter(!sources.contains(_))}")

      }

      nestedComp(e).filter(c => variables(c).filter(!sources.contains(_)).subsetOf(compIdns(c)))
  }

  private lazy val ruleInnerPrimitiveReduceOnPredicate = rule[Exp] {
    case c @ CanComp(m, gs, ps, e) if ps.flatMap(nestedIndependentComp).nonEmpty =>
      logger.debug(s"Applying ruleInnerPrimitiveReduceOnPredicate on ${CalculusPrettyPrinter(c)}")

      var sym: Symbol = null
      var nc: CanComp = null
      val nps = for (p <- ps) yield {
        val ncs = nestedIndependentComp(p)
        if (sym == null && ncs.nonEmpty) {
          nc = ncs.head
          val nc1 = ncs.head
          sym = SymbolTable.next()
          rewrite(
            everywhere(rule[Exp] {
              case x if x eq nc1 => IdnExp(IdnUse(sym.idn))
            }))(p)
        } else {
          p
        }
      }

      val r = ExpBlock(
        Seq(Bind(PatternIdn(IdnDef(sym.idn)), nc)),
        CanComp(m, gs, nps, e)
      )
      logger.debug(s"Output is ${CalculusPrettyPrinter(r)}")
      r
//
//
//      for (p <- ps if nc == null) {
//        val c1 = nestedComp(p)
//        if (c1.nonEmpty) {
//          nc = c1.head
//        }
//      }
//
//
//      val binds = ps.flatMap {
//        case p => nestedComp(p).map { case r => r -> SymbolTable.next().idn }
//      }
//      val nps = scala.collection.mutable.MutableList[Exp]()
//      for (p <- ps) {
//        var np = p
//        for ((r, idn) <- binds) {
//          np = rewrite(
//            everywhere(rule[Exp] {
//              case `r` => IdnExp(IdnUse(idn))
//            }))(np)
//        }
//        nps += np
//      }
//      ExpBlock(
//        binds.map { case (r, idn) => Bind(PatternIdn(IdnDef(idn)), r) },
//        CanComp(m, gs, nps.to, e)
//      )
  }

  /**
   *
   */
  private lazy val ruleReduceExpBlock = rule[Exp] {
    case ExpBlock(bs, CanComp(m, g :: Nil, ps, e)) =>
      logger.debug(s"Applying ruleReduceExpBlock")
      ExpBlock(
        bs,
        CanComp(m, Seq(Gen(deepclone(g.p), Filter(g, foldPreds(ps)))), Nil, e)
      )
  }

  /** Create a record expression from a pattern (used by Nest).
    */
  def createRecord(u: Pattern): Exp = u match {
    case PatternProd(ps) => RecordCons(ps.zipWithIndex.map { case (np, i) => AttrCons(s"_${i + 1}", createRecord(np))})
    case PatternIdn(idn) => IdnExp(IdnUse(idn.idn))
  }

  // TODO: What happens with nested predicate? How/when to make the ExpBlock?

  /** The set of variables used in an expression.
    */
  // TODO: Turn into attribute?
  def variables(e: Exp): Set[Idn] = {
    val collectIdns = collect[Set, Idn]{ case IdnExp(idn) => idn.idn }
    collectIdns(e)
  }

  /** Fold a sequence of predicates into a single ANDed predicate.
    */
  def foldPreds(ps: Seq[Exp]) =
    ps.foldLeft(BoolConst(true).asInstanceOf[Exp])((a, b) => MergeMonoid(AndMonoid(), a, b))

//
//
//
//  lazy val unnest = rule[Exp] {


//    case c: CanComp => recurse(c, None, None, nested=false)
//  }
//
//  // TODO: We now believe the "Nested comprehension on a predicate" rule can be replaced by an ExpBlock with
//  //       a bind that pre-computes the output of the nested comprehension and rewrites it in the predicate.
//  // TODO: We also believe the Nest doesn't need to check specifically for the gs IF we have support for options and
//  //      null propagation in our code, since that's handled by the same code. In Fegaras original examples, since the
//  //      core language is not expressive enough for null values, Fegaras needs to carry around that state to handle it
//  //      later properly when building the Nest.
//
//  private def recurse(c: CanComp, args: Option[Pattern], child: Option[Exp], nested: Boolean): Exp = (c, args, child) match {
//
//    /** Nested comprehension on a predicate
//      */
//
//    case (CanComp(m, gs, ps, e), Some(w), _) if hasNestedComp(ps) && areIndependent(getNestedComp(ps), gs) =>
//      logger.debug(s"Unnesting nested comprehension on a predicate")
//      val c = getNestedComp(ps)
//      val sym_v = SymbolTable.next()
//      val pat_v = PatternIdn(IdnDef(sym_v.idn))
//      val pat_w_v = PatternProd(Seq(w, pat_v))
//      val nps = ps.map(rewrite(attempt(oncetd(rule[Exp] {
//        case `c` => IdnExp(IdnUse(sym_v.idn))
//      }))))
//      val nchild = recurse(c, Some(w), child, nested = true)
//      recurse(CanComp(m, gs, nps, e), Some(pat_w_v), Some(nchild), nested = nested)
//
//    /** Nested comprehension on the projected expression
//      */
//    case (CanComp(m, Nil, p, f @ NestedComp(c1)), Some(w), _) =>
//      logger.debug(s"Unnesting nested comprehension on the projected expression")
//      val sym_v = SymbolTable.next()
//      val pat_v = PatternIdn(IdnDef(sym_v.idn))
//      val pat_w_v = PatternProd(Seq(w, pat_v))
//      val nf = rewrite(oncetd(rule[Exp] {
//        case `c1` => IdnExp(IdnUse(sym_v.idn))
//      }))(f)
//      val nchild = recurse(c1, Some(w), child, nested = true)
//      recurse(CanComp(m, Nil, p, nf), Some(pat_w_v), Some(nchild), nested = nested)
//

//    /** Reduce/Nest
//      */
//    case (CanComp(m, Nil, ps, e), Some(w), Some(child)) =>
//      val nsym = SymbolTable.next()
//      val nps = foldPreds(ps.map(rewriteExp(_, w, nsym.idn)))
//      val ne = rewriteExp(e, w, nsym.idn)
//
//      if (!nested) {
//        logger.debug(s"Creating Reduce")
//        Reduce(m, Gen(PatternIdn(IdnDef(nsym.idn)), child), nps, ne)
//      } else {
//        logger.debug(s"Creating Nest")
//        val nf =
//        Nest(m, Gen(PatternIdn(IdnDef(nsym.idn)), child), nps, ne, FunAbs(w, createRecord(u, w)))
//      }
//
//    /** (Outer)Join
//      */
//
//    case (CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), right: IdnExp) :: r, ps, e), Some(w), Some(left)) =>
//      logger.debug(s"Applying unnester rule C6")
//      val pat_w_v = PatternProd(Seq(w, pat_v))
//      val ps_v = ps.filter(variables(_) == Set(v))
//      val ps_w_v = ps.filter(pred => !ps_v.contains(pred) && variables(pred).subsetOf(getIdns(pat_w_v).toSet))
//      val ps_rest = ps.filter(pred => !ps_v.contains(pred) && !ps_w_v.contains(pred))
//
//      val nsym_v = SymbolTable.next()
//      val nps_v = foldPreds(ps_v.map(rewriteExp(_, pat_v, nsym_v.idn)))
//      val npat_v = PatternIdn(IdnDef(nsym_v.idn))
//
//      val nsym_w = SymbolTable.next()
//      val npat_w = PatternIdn(IdnDef(nsym_w.idn))
//
//      val nsym_w_v = SymbolTable.next()
//      val npat_w_v = PatternProd(Seq(npat_w, npat_v))
//
//      val nps_w_v = foldPreds(ps_w_v.map(rewriteExp(_, pat_w_v, nsym_w_v.idn)))
////
////      rewrite twice?
////      once rewrite the w another rewrite the v?
////      this is... confusing
////
////        for each idn, rewrite by the one on the right or on the left
//
//      if (!nested) {
//        recurse(CanComp(m, r, ps_rest, e), None, Some(pat_w_v), Some(Join(Gen(npat_w, left), Filter(Gen(npat_v, right), nps_v), nps_w_v)))
//      } else {
//
//        //    case CalculusTerm(CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x: IdnExp) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
//        //      logger.debug(s"Applying unnester rule C9")
//        //      val pat_w_v = PatternProd(Seq(w, pat_v))
//        //      val pred_v = p.filter(variables(_) == Set(v))
//        //      val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(getIdns(pat_w_v).toSet))
//        //      val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
//        //
//        //
//        //      why do i need to propagate the nulls?
//        //
//        //
//        //      recurse(CalculusTerm(CanComp(m, r, pred_rest, e), Some(u), Some(pat_w_v), AlgebraTerm(OuterJoin(FunAbs(pat_w_v, foldPreds(pred_w_v)), child, Filter(FunAbs(pat_v, foldPreds(pred_v)), x)))))
//        //
//      }
//
//    /** (Outer)Unnest
//      */
//
//        case CalculusTerm(CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
//          logger.debug(s"Applying unnester rule C7")
//          val pat_w_v = PatternProd(Seq(w, pat_v))
//          val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
//
//    //        here it makes me think we should move the filter out of the unnest
//    //        if there any theoretical adv in having it there? not quite; except it is needed for the outer unnest
//    //        how to represent it?
//    //
//    //        two Gens?
//    //
//    //        $44 <- Students, $45 <- $44.courses (?)
//    //
//    //        ok
//          if (!nested) {
//            recurse(CalculusTerm(CanComp(m, r, pred_not_v, e), None, Some(pat_w_v), AlgebraTerm(Unnest(FunAbs(w, path), FunAbs(pat_w_v, foldPreds(pred_v)), child))))
//          } else {
//            //    case (CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path) :: r, p, e), Some(u), Some(w), Some(child)) =>
//            //      logger.debug(s"Applying unnester rule C10")
//            //      val pat_w_v = PatternProd(Seq(w, pat_v))
//            //      val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
//            //      recurse(CanComp(m, r, pred_not_v, e), Some(u), Some(pat_w_v), OuterUnnest(FunAbs(w, path), FunAbs(pat_w_v, foldPreds(pred_v)), child))
//
//          }
//  }
//
//
//  /** The set of variables used in an expression.
//    */
//  def variables(e: Exp): Set[String] = {
//    val collectIdns = collect[Set, String]{ case IdnExp(idn) => idn.idn }
//    collectIdns(e)
//  }
//
//
//  def rewriteExp(e: Exp, p: Pattern, newIdn: Idn) =
//    rewrite(oncetd(rule[Exp] {
//      case e1 @ IdnExp(IdnUse(idn)) =>
//        projectIdn(p, idn, newIdn) match {
//          case Some(ne) => ne
//          case _ => e1
//        }
//    }))(e)
//
//  /** Build an expression that projects the identifier given the pattern.
//    */
//  def projectIdn(p: Pattern, idn: Idn, newIdn: Idn): Option[Exp] = {
//    def recurse(p: Pattern): Option[Exp] = p match {
//      case PatternIdn(IdnDef(`idn`)) => Some(IdnExp(IdnUse(newIdn)))
//      case _: PatternIdn => None
//      case PatternProd(ps) =>
//        for ((p, i) <- ps.zipWithIndex) {
//          if (recurse(p).isDefined) {
//            return Some(RecordProj(recurse(p).head, s"_${i + 1}"))
//          }
//        }
//        None
//    }
//
//    recurse(p)
//  }
//
//  /** Fold a sequence of predicates into a single ANDed predicate.
//    */
//  def foldPreds(ps: Seq[Exp]) =
//    ps.foldLeft(BoolConst(true).asInstanceOf[Exp])((a, b) => MergeMonoid(AndMonoid(), a, b))
//
//  /** Create a record expression from a pattern (used by Nest).
//    */
//  def createRecord(u: Pattern, p: Pattern): Exp = u match {
//    case PatternProd(ps) => RecordCons(ps.zipWithIndex.map { case (np, i) => AttrCons(s"_${i + 1}", createRecord(np, p))})
//    case PatternIdn(idn) => IdnExp(IdnUse(idn.idn))
//  }
//
////  /** Return a new pattern which corresponds to w minus u (used by Nest).
////    */
////  def subtractPattern(w: Pattern, u: Pattern): Exp = {
////    val us = getIdns(u)
////    val nws = getIdns(w).filter { case idn => !us.contains(idn) }
////    RecordCons(nws.zipWithIndex.map { case (nw, i) => AttrCons(s"_${i + 1}", IdnExp(IdnUse(nw))) })
////  }
//
//  /** Return the sequence of identifiers used in a pattern.
//    */
//  def getIdns(p: Pattern): Seq[String] = p match {
//    case PatternProd(ps) => ps.flatMap(getIdns)
//    case PatternIdn(IdnDef(idn))   => Seq(idn)
//  }
//
//  /** Returns true if the comprehension `c` does not depend on `s` generators.
//    */
//  def areIndependent(c: CanComp, s: Seq[Gen]) = {
//    // TODO: MAKE THIS SUPPORT PATTERNS (PROD+IDN) INSTEAD OF ONLY PATTERN IDNS
//    val sVs = s.map { case Gen(PatternIdn(IdnDef(v)), _) => v }.toSet
//    variables(c).intersect(sVs).isEmpty
//  }
//
//  // TODO: hasNestedComp followed by getNestedComp is *slow* code; replace by extractor
//  def hasNestedComp(ps: Seq[Exp]) =
//    ps.collectFirst { case NestedComp(c) => c }.isDefined
//
//  def getNestedComp(ps: Seq[Exp]) =
//    ps.collectFirst { case NestedComp(c) => c }.head
//
//  /** Extractor to pattern match nested comprehensions.
//    */
//  private object NestedComp {
//    def unapply(e: Exp): Option[CanComp] = e match {
//      case RecordProj(NestedComp(e1), _)           => Some(e1)
//      case RecordCons(atts)                        => atts.flatMap(att => att.e match {
//        case NestedComp(e1) => Some(e1)
//        case _              => None
//      }).headOption
//      case IfThenElse(NestedComp(e1), _, _)        => Some(e1)
//      case IfThenElse(_, NestedComp(e2), _)        => Some(e2)
//      case IfThenElse(_, _, NestedComp(e3))        => Some(e3)
//      case BinaryExp(_, NestedComp(e1), _)         => Some(e1)
//      case BinaryExp(_, _, NestedComp(e2))         => Some(e2)
//      case ConsCollectionMonoid(_, NestedComp(e1)) => Some(e1)
//      case MergeMonoid(_, NestedComp(e1), _)       => Some(e1)
//      case MergeMonoid(_, _, NestedComp(e2))       => Some(e2)
//      case UnaryExp(_, NestedComp(e1))             => Some(e1)
//      case c: CanComp                              => Some(c)
//      case _: IdnExp => None
////      case _                                       => None
//    }
//  }
}

object Unnester {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree)
    val a = new SemanticAnalyzer(t1, world)
    val unnester = new Unnester {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(unnester.strategy)(t1)
  }
}


//
//object Unnester extends LazyLogging {
//
//  def apply(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode = {
//    val tree1 = Simplifier(tree, world)
//
//    val analyzer = new SemanticAnalyzer(tree1, world)
//
//    val collectEntities = collect[List, Tuple2[String, Entity]]{ case Calculus.IdnExp(idn) => idn.idn -> analyzer.entity(idn) }
//    val entities = collectEntities(tree1.root).toMap
//
//    val collectTypes = collect[List, Tuple2[Calculus.Exp, Type]]{ case e: Calculus.Exp => e -> analyzer.tipe(e) }
//    val tipes = collectTypes(tree1.root).toMap
//
//    logger.debug(s"Unnester input tree: ${calculus.CalculusPrettyPrinter(tree1.root)}")
//
//    def unnest(e: Calculus.Exp): LogicalAlgebra.LogicalAlgebraNode = {
//
//      /** Extract object to pattern match scan objects.
//        */
//      object Scan {
//        def unapply(e: Calculus.Exp): Option[LogicalAlgebra.Scan] = e match {
//          case Calculus.IdnExp(idn) => entities(idn.idn) match {
//            case e@SymbolTable.DataSourceEntity(sym) => Some(LogicalAlgebra.Scan(sym.idn, world.sources(sym.idn))) // TODO: Use more precise type if one was inferred
//            case _ => None
//          }
//          case _                    => None
//        }
//      }
//
//
//
//      /** Return the inner type of a collection.
//        */
//      def getInnerType(t: Type): Type = t match {
//        case c: CollectionType => c.innerType
//        //        case UserType(idn)     => getInnerType(world.userTypes(idn))
//        case _                 => throw UnnesterError(s"Expected collection type but got $t")
//      }
//
//      /** Return the set of variable used in an expression.
//        */
//      def variables(e: Calculus.Exp): Set[String] = {
//        val collectIdns = collect[Set, String]{ case Calculus.IdnExp(idn) => idn.idn }
//        collectIdns(e)
//      }
//
//      /** Build type from pattern.
//        */
//      def patternType(p: Pattern): Type = p match {
//        case PairPattern(p1, p2) => RecordType(List(AttrType("_1", patternType(p1)), AttrType("_2", patternType(p2))), None)
//        case IdnPattern(p1, t)   => t
//      }
//

//
//      /** Convert canonical calculus expression to algebra expression.
//        * The position of each canonical expression variable is used as the argument.
//        */
//      def createExp(e: Calculus.Exp, p: Pattern): Expressions.Exp = {
//        def recurse(e: Calculus.Exp): Expressions.Exp = e match {
//          case _: Calculus.Null                                 => Expressions.Null
//          case Calculus.BoolConst(v)                            => Expressions.BoolConst(v)
//          case Calculus.IntConst(v)                             => Expressions.IntConst(v)
//          case Calculus.FloatConst(v)                           => Expressions.FloatConst(v)
//          case Calculus.StringConst(v)                          => Expressions.StringConst(v)
//          case Calculus.IdnExp(idn)                             => buildPatternExp(idn.idn, p)
//          case Calculus.RecordProj(e1, idn)                     => Expressions.RecordProj(recurse(e1), idn)
//          case Calculus.RecordCons(atts)                        => Expressions.RecordCons(atts.map { att => Expressions.AttrCons(att.idn, recurse(att.e)) })
//          case Calculus.IfThenElse(e1, e2, e3)                  => Expressions.IfThenElse(recurse(e1), recurse(e2), recurse(e3))
//          case Calculus.BinaryExp(op, e1, e2)                   => Expressions.BinaryExp(op, recurse(e1), recurse(e2))
//          case Calculus.MergeMonoid(m: PrimitiveMonoid, e1, e2) => Expressions.MergeMonoid(m, recurse(e1), recurse(e2))
//          case Calculus.UnaryExp(op, e1)                        => Expressions.UnaryExp(op, recurse(e1))
//          case n                                                => throw UnnesterError(s"Unexpected node: $n")
//        }
//
//        recurse(e)
//      }
//
//      /** Create a predicate, which is an expression in conjunctive normal form.
//        */
//      def createPredicate(ps: Seq[Calculus.Exp], pat: Pattern): Expressions.Exp = {
//        ps match {
//          case Nil          => Expressions.BoolConst(true)
//          case head :: Nil  => createExp(head, pat)
//          case head :: tail => tail.map(createExp(_, pat)).foldLeft(createExp(head, pat))((a, b) => Expressions.MergeMonoid(AndMonoid(), a, b))
//        }
//      }
//
//
//
//
//      def freshIdn = SymbolTable.next().idn
//
//
//
//    }
//
//    unnest(tree1.root)
//  }
//
//}
//
