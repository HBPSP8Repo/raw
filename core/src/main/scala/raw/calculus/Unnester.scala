package raw
package calculus

import Calculus._

import org.kiama.attribution.Attribution

case class UnnesterError(err: String) extends RawException(err)

/** Terms used during query unnesting.
  */
sealed abstract class Term
case object EmptyTerm extends Term
case class CalculusTerm(c: Calculus.Exp, u: Option[Pattern], w: Option[Pattern], child: Term) extends Term
case class AlgebraTerm(t: LogicalAlgebraNode) extends Term

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
class Unnester extends Attribution with Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._

  def strategy = unnester

  private lazy val unnester = manytd(unnest)

  private lazy val unnest = rule[Exp] {
    case c: CanComp =>
      logger.debug(s"Unnesting ${CalculusPrettyPrinter(c)}")
      recurse(CalculusTerm(c, None, None, EmptyTerm)) match {
        case AlgebraTerm(a) => a
        case o              => throw UnnesterError(s"Invalid output: $o")
      }
  }

  private def recurse(t: Term): Term = t match {

    /** Rule C11: Nested predicate in comprehension
      */

    case CalculusTerm(n @ CanComp(m, s, p, e1), u, Some(w), child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) =>
      logger.debug(s"Applying unnester rule C11 to ${CalculusPrettyPrinter(n)}")
      val c = getNestedComp(p)
      val v = SymbolTable.next()
      val pat_v = PatternIdn(IdnDef(v.idn))
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val npred = p.map(rewrite(attempt(oncetd(rule[Exp] {
        case `c` => IdnExp(IdnUse(v.idn))
      })))(_))
      recurse(CalculusTerm(CanComp(m, s, npred, e1), u, Some(pat_w_v), recurse(CalculusTerm(c, Some(w), Some(w), child))))

    /** Rule C12: Nested comprehension in the projection
      */

    case CalculusTerm(n @ CanComp(m, Nil, p, f), u, Some(w), child) if hasNestedComp(Seq(f)) =>
      logger.debug(s"Applying unnester rule C12 to ${CalculusPrettyPrinter(n)}")
      val c = getNestedComp(Seq(f))
      val v = SymbolTable.next()
      val pat_v = PatternIdn(IdnDef(v.idn))
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val nf = rewrite(oncetd(rule[Exp] {
        case `c` => IdnExp(IdnUse(v.idn))
      }))(f)
      recurse(CalculusTerm(CanComp(m, Nil, p, nf), u, Some(pat_w_v), recurse(CalculusTerm(c, Some(w), Some(w), child))))


    /** Rule C7: Unnest
      */

    case CalculusTerm(n @ CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path: RecordProj) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C7 to ${CalculusPrettyPrinter(n)}")
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
      recurse(CalculusTerm(CanComp(m, r, pred_not_v, e), None, Some(pat_w_v), AlgebraTerm(Unnest(Gen(w, child), Gen(pat_v, path), foldPreds(pred_v)))))

    /** Rule C4: Scan/Filter
      */

    case CalculusTerm(n @ CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x) :: r, p, e), None, None, EmptyTerm) =>
      logger.debug(s"Applying unnester rule C4 to ${CalculusPrettyPrinter(n)}")
      val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
      recurse(CalculusTerm(CanComp(m, r, pred_not_v, e), None, Some(pat_v), AlgebraTerm(Filter(Gen(pat_v, x), foldPreds(pred_v)))))

    /** Rule C6: Join
      */

    case CalculusTerm(n @ CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x) :: r, p, e), None, Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C6 to ${CalculusPrettyPrinter(n)}")
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val pred_v = p.filter(variables(_) == Set(v))
      val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(idns(pat_w_v).toSet))
      val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
      recurse(CalculusTerm(CanComp(m, r, pred_rest, e), None, Some(pat_w_v), AlgebraTerm(Join(Gen(w, child), Gen(pat_v, Filter(Gen(deepclone(pat_v), x), foldPreds(pred_v))), foldPreds(pred_w_v)))))

    /** Rule C5: Reduce
      */

    case CalculusTerm(n @ CanComp(m, Nil, p, e), None, Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C5 to ${CalculusPrettyPrinter(n)}")
      AlgebraTerm(Reduce(m, Gen(w, Filter(Gen(deepclone(w), child), foldPreds(p))), e))

    /** Rule C10: OuterUnnest
      */

    case CalculusTerm(n @ CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), path: RecordProj) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C10 to ${CalculusPrettyPrinter(n)}")
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val (pred_v, pred_not_v) = p.partition(variables(_) == Set(v))
      recurse(CalculusTerm(CanComp(m, r, pred_not_v, e), Some(u), Some(pat_w_v), AlgebraTerm(OuterUnnest(Gen(w, child), Gen(pat_v, path), foldPreds(pred_v)))))

    /** Rule C9: OuterJoin
      */

    case CalculusTerm(n @ CanComp(m, Gen(pat_v @ PatternIdn(IdnDef(v)), x) :: r, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C9 to ${CalculusPrettyPrinter(n)}")
      val pat_w_v = PatternProd(Seq(w, pat_v))
      val pred_v = p.filter(variables(_) == Set(v))
      val pred_w_v = p.filter(pred => !pred_v.contains(pred) && variables(pred).subsetOf(idns(pat_w_v).toSet))
      val pred_rest = p.filter(pred => !pred_v.contains(pred) && !pred_w_v.contains(pred))
      logger.debug(s"pred_v is $pred_v")
      logger.debug(s"pred_w_v is $pred_w_v")
      logger.debug(s"pred_rest is $pred_rest")
      logger.debug(s"idns(pat_w_v) ${idns(pat_w_v)}")
      for (pred <- p) {
        logger.debug(s"pred is ${CalculusPrettyPrinter(pred)}")
        logger.debug(s"  vars in pred ${variables(pred)}")
        logger.debug(s" cond1 ${!pred_v.contains(pred)}")
        logger.debug(s" cond2 ${variables(pred).subsetOf(idns(pat_w_v).toSet)}")
      }
      recurse(CalculusTerm(CanComp(m, r, pred_rest, e), Some(u), Some(pat_w_v), AlgebraTerm(OuterJoin(Gen(w, child), Gen(pat_v, Filter(Gen(deepclone(pat_v), x), foldPreds(pred_v))), foldPreds(pred_w_v)))))

    /** Rule C8: Nest
      */

    case CalculusTerm(n @ CanComp(m, Nil, p, e), Some(u), Some(w), AlgebraTerm(child)) =>
      logger.debug(s"Applying unnester rule C8 to ${CalculusPrettyPrinter(n)}")
      AlgebraTerm(Nest(m, Gen(w, child), createRecord(u), foldPreds(p), e))
  }

  /** Fold predicates into single ANDed predicate.
    */
  private def foldPreds(ps: Seq[Exp]) =
    if (ps.isEmpty)
      BoolConst(true)
    else if (ps.length == 1)
      ps.head
    else
      ps.tail.foldLeft(ps.head)((a, b) => MergeMonoid(AndMonoid(), a, b))

  /** Create a record expression from a pattern (used by Nest).
    */
  private def createRecord(u: Pattern): Exp = u match {
    case PatternProd(ps) => RecordCons(ps.zipWithIndex.map { case (np, i) => AttrCons(s"_${i + 1}", createRecord(np)) })
    case PatternIdn(idn) => IdnExp(IdnUse(idn.idn))
  }

  /** Return the sequence of identifiers used in a pattern.
    */
  lazy val idns: Pattern => Seq[String] = attr {
    case PatternProd(ps)         => ps.flatMap(idns)
    case PatternIdn(IdnDef(idn)) => Seq(idn)
  }
//  private def idns(p: Pattern): Seq[String] = p match {
//    case PatternProd(ps)         => ps.flatMap(idns)
//    case PatternIdn(IdnDef(idn)) => Seq(idn)
//  }

  /** Returns true if the comprehension `c` does not depend on `s` generators.
    */
  private def areIndependent(c: CanComp, s: Seq[Gen]) = {
    val sVs: Set[String] = s.map { case Gen(PatternIdn(IdnDef(v)), _) => v }.toSet
    variables(c).intersect(sVs).isEmpty
  }

  private lazy val nestedComp: Exp => Seq[CanComp] = attr {
    case e: Exp =>
      val collectComps = collect[Seq, CanComp] {
        case n: CanComp => n
      }
      collectComps(e)
  }

  private def hasNestedComp(ps: Seq[Exp]) = ps.flatMap(nestedComp).nonEmpty

  private def getNestedComp(ps: Seq[Exp]) = ps.flatMap(nestedComp).head

  /** Returns the set of identifiers in an expression.
    */
  private def variables(e: Exp): Set[String] = {
    val collectIdns = collect[Set, String] {
      case idn: IdnNode => idn.idn
    }
    collectIdns(e)
  }
}
