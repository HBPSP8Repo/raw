package raw
package algebra

import scala.collection.immutable.Seq
import com.typesafe.scalalogging.LazyLogging
import calculus.{Calculus, SymbolTable}

case class UnnesterError(err: String) extends RawException(err)

// TODO: Why not just change logical to physical algebra here, instead of having to create a different tree for "such" a small transformation?

/** Patterns
  */
sealed abstract class Pattern
case class PairPattern(fst: Pattern, snd: Pattern) extends Pattern
case class IdnPattern(idn: Calculus.IdnNode) extends Pattern
case object EmptyPattern extends Pattern

/** Terms used during query unnesting.
  */
sealed abstract class Term
case object EmptyTerm extends Term
case class CalculusTerm(c: Calculus.Exp, u: Pattern, w: Pattern, child: Term) extends Term
case class AlgebraTerm(t: LogicalAlgebra.LogicalAlgebraNode) extends Term

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
object Unnester extends LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy

  def apply(tree: Calculus.Calculus, world: World): LogicalAlgebra.LogicalAlgebraNode = {
    val inTree = calculus.Simplifier(tree, world)
    logger.debug(s"Unnester input tree: ${calculus.CalculusPrettyPrinter(inTree.root)}")

    val analyzer = new calculus.SemanticAnalyzer(inTree, world)

    def unnest(e: Calculus.Exp): LogicalAlgebra.LogicalAlgebraNode = {
      unnesterRules(CalculusTerm(e, EmptyPattern, EmptyPattern, EmptyTerm)) match {
        case Some(AlgebraTerm(a)) => a
        case o                    => throw UnnesterError(s"Invalid output: $o")
      }
    }

    lazy val unnesterRules: Strategy =
      reduce(ruleC11 < unnesterRules + (ruleC12 < unnesterRules + (ruleC4 <+ ruleC5 <+ ruleC6 <+ ruleC7 <+ ruleC8 <+ ruleC9 <+ ruleC10 + ruleTopLevelMerge)))

    /** Extract object to pattern match scan objects.
      */
    object ExtractScalaObject {
      def unapply(e: Calculus.Exp): Option[LogicalAlgebra.Scan] = e match {
        case Calculus.IdnExp(idn)                                          => analyzer.entity(idn) match {
            // TODO: Check if asInstanceOf works and fail gracefully if not!
          case SymbolTable.DataSourceEntity(s @ ScalaDataSource(obj)) => Some(LogicalAlgebra.Scan(obj.asInstanceOf[Iterable[Any]], s.tipe))
          case SymbolTable.DataSourceEntity(EmptyDataSource(t)) => Some(LogicalAlgebra.Scan(List(), t))
          case _ => None
        }
//        case Calculus.FunApp(Calculus.IdnExp(idn), e: Calculus.RecordCons) => analyzer.entity(idn) match {
//          case SymbolTable.ScalaEntity(obj) => Some(LogicalAlgebra.Scan(obj, Some(createExp(e, EmptyPattern).asInstanceOf[Expressions.RecordCons]))) // TODO: Ensure that 'e' does not reference variables! Do it elsewhere in the semantic analyzer?
//          case _ => None
//        }
        case _                                                             => None
      }
    }

    /** Extract object to pattern match comprehensions in the canonical form.
      */
    object CanonicalComp {

      import Calculus._

      def apply(m: Monoid, paths: List[Gen], preds: List[Exp], e: Exp): Comp =
        Comp(m, paths ++ preds, e)

      def unapply(c: Comp): Option[(Monoid, List[Gen], List[Exp], Exp)] = {
        val paths = c.qs.collect { case g: Gen => g}
        val preds = c.qs.collect { case e: Exp => e}
        Some(c.m, paths.toList, preds.toList, c.e)
      }
    }

    /** Return the set of variable used in an expression.
      */
    def variables(e: Calculus.Exp): Set[Calculus.IdnNode] = {
      var vs = scala.collection.mutable.Set[Calculus.IdnNode]()
      everywhere(query[Calculus.Exp] { case Calculus.IdnExp(idn) => vs += idn})(e)
      vs.toSet
    }

    def buildPatternExp(idn: Calculus.IdnNode, p: Pattern): Expressions.Exp = {
      def recurse(p: Pattern, e: Expressions.Exp): Option[Expressions.Exp] = p match {
        case IdnPattern(idn1) if idn.idn == idn1.idn => Some(e)
        case PairPattern(fst, snd) =>
          recurse(fst, Expressions.RecordProj(e, "_1")) match {
            case Some(e1) => Some(e1)
            case _ => recurse(snd, Expressions.RecordProj(e, "_2")) match {
              case Some(e2) => Some(e2)
              case _        => None
            }
          }
        case _ => None
      }

      recurse(p, Expressions.Arg).head
    }

    /** Convert canonical calculus expression to algebra expression.
      * The position of each canonical expression variable is used as the argument.
      */
    def createExp(e: Calculus.Exp, p: Pattern): Expressions.Exp = {
      def recurse(e: Calculus.Exp): Expressions.Exp = e match {
        case _: Calculus.Null                     => Expressions.Null
        case Calculus.BoolConst(v)                => Expressions.BoolConst(v)
        case Calculus.IntConst(v)                 => Expressions.IntConst(v)
        case Calculus.FloatConst(v)               => Expressions.FloatConst(v)
        case Calculus.StringConst(v)              => Expressions.StringConst(v)
        case Calculus.IdnExp(idn)                 => buildPatternExp(idn, p)
        case Calculus.RecordProj(e1, idn)         => Expressions.RecordProj(recurse(e1), idn)
        case Calculus.RecordCons(atts)            => Expressions.RecordCons(atts.map { att => Expressions.AttrCons(att.idn, recurse(att.e))})
        case Calculus.IfThenElse(e1, e2, e3)      => Expressions.IfThenElse(recurse(e1), recurse(e2), recurse(e3))
        case Calculus.BinaryExp(op, e1, e2)       => Expressions.BinaryExp(op, recurse(e1), recurse(e2))
        case Calculus.ZeroCollectionMonoid(m)     => Expressions.ZeroCollectionMonoid(m)
        case Calculus.ConsCollectionMonoid(m, e1) => Expressions.ConsCollectionMonoid(m, recurse(e1))
        case Calculus.MergeMonoid(m, e1, e2)      => Expressions.MergeMonoid(m, recurse(e1), recurse(e2))
        case Calculus.UnaryExp(op, e1)            => Expressions.UnaryExp(op, recurse(e1))
        case n                                    => throw UnnesterError(s"Unexpected node: $n")
      }

      recurse(e)
    }

    // Create predicate, which is an expression in conjunctive normal form.
    def createPredicate(ps: Seq[Calculus.Exp], pat: Pattern): Expressions.Exp = {
      ps match {
        case Nil          => Expressions.BoolConst(true)
        case head :: Nil  => createExp(head, pat)
        case head :: tail => tail.map(createExp(_, pat)).foldLeft(createExp(head, pat))((a, b) => Expressions.MergeMonoid(AndMonoid(), a, b))
      }
    }

    def createRecord(idns: Seq[Calculus.IdnNode], pat: Pattern): Expressions.Exp =
      Expressions.RecordCons(idns.zipWithIndex.map { case (idn, idx) => Expressions.AttrCons(s"_${idx + 1}", buildPatternExp(idn, pat)) })

//      // TODO: ???
//      Expressions.ProductCons(idns.map{ idn => Expressions.ProductProj(Expressions.Arg, vs.indexOf(idn)) })

    /** Rule C4
      */

    lazy val ruleC4 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Calculus.Gen(v, ExtractScalaObject(x)) :: r, p, e), EmptyPattern, EmptyPattern, EmptyTerm) => {
        logger.debug(s"Applying unnester rule C4")
        val (p_v, p_not_v) = p.partition(variables(_).map(_.idn) == Set(v.idn))
        CalculusTerm(CanonicalComp(m, r, p_not_v, e), EmptyPattern, IdnPattern(v), AlgebraTerm(LogicalAlgebra.Select(createPredicate(p_v, IdnPattern(v)), x)))
      }
    }

    /** Rule C5
      */

    lazy val ruleC5 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Nil, p, e), EmptyPattern, w, AlgebraTerm(child)) => {
        logger.debug(s"Applying unnester rule C5")
        AlgebraTerm(LogicalAlgebra.Reduce(m, createExp(e, w), createPredicate(p, w), child))
      }
    }

    /** Rule C6
      */

    def getIdns(p: Pattern): Seq[Calculus.IdnNode] = p match {
      case PairPattern(p1, p2) => getIdns(p1) ++ getIdns(p2)
      case IdnPattern(p1) => Seq(p1)
      case EmptyPattern => Seq()
    }

    lazy val ruleC6 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Calculus.Gen(v, ExtractScalaObject(x)) :: r, p, e), EmptyPattern, w, AlgebraTerm(child)) => {
        logger.debug(s"Applying unnester rule C6")
        val p_v = p.filter(variables(_).map(_.idn) == Set(v.idn))
        val p_w_v = p.filter(pred => getIdns(PairPattern(w, IdnPattern(v))).toSet.map{idnNode: Calculus.IdnNode => idnNode.idn}.subsetOf(variables(pred).map{a => a.idn}))
        val p_rest = p.filter(pred => !p_v.contains(pred) && !p_w_v.contains(pred))
        CalculusTerm(CanonicalComp(m, r, p_rest, e), EmptyPattern, PairPattern(w, IdnPattern(v)), AlgebraTerm(LogicalAlgebra.Join(createPredicate(p_w_v, PairPattern(w, IdnPattern(v))), child, LogicalAlgebra.Select(createPredicate(p_v, IdnPattern(v)), x))))
      }
    }

    /** Rule C7
      */

    lazy val ruleC7 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Calculus.Gen(v, path) :: r, p, e), EmptyPattern, w, AlgebraTerm(child)) => {
        logger.debug(s"Applying unnester rule C7")
        val (p_v, p_not_v) = p.partition(variables(_).map(_.idn) == Set(v.idn))
        CalculusTerm(CanonicalComp(m, r, p_not_v, e), EmptyPattern, PairPattern(w, IdnPattern(v)), AlgebraTerm(LogicalAlgebra.Unnest(createExp(path, w), createPredicate(p_v, PairPattern(w, IdnPattern(v))), child)))
      }
    }

    /** Rule C8
      */

    lazy val ruleC8 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Nil, p, e), u, w, AlgebraTerm(child)) if u != EmptyPattern => {
        logger.debug(s"Applying unnester rule C8")
        AlgebraTerm(LogicalAlgebra.Nest(m, createExp(e, w), createRecord(getIdns(u), w), createPredicate(p, w), createRecord(getIdns(w).filterNot(getIdns(u).contains), w), child))
      }
    }

    /** Rule C9
      */

    lazy val ruleC9 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Calculus.Gen(v, ExtractScalaObject(x)) :: r, p, e), u, w, AlgebraTerm(child)) if u != EmptyPattern => {
        logger.debug(s"Applying unnester rule C9")
        val p_v = p.filter(variables(_).map(_.idn) == Set(v.idn))
        val p_w_v = p.filter(pred => getIdns(PairPattern(w, IdnPattern(v))).toSet.map{idnNode: Calculus.IdnNode => idnNode.idn}.subsetOf(variables(pred).map(_.idn)))
        val p_rest = p.filter(pred => !p_v.contains(pred) && !p_w_v.contains(pred))
        CalculusTerm(CanonicalComp(m, r, p_rest, e), u, PairPattern(w, IdnPattern(v)), AlgebraTerm(LogicalAlgebra.OuterJoin(createPredicate(p_w_v, PairPattern(w, IdnPattern(v))), child, LogicalAlgebra.Select(createPredicate(p_v, IdnPattern(v)), x))))
      }
    }

    /** Rule C10
      */

    lazy val ruleC10 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Calculus.Gen(v, path) :: r, p, e), u, w, AlgebraTerm(child)) if u != EmptyPattern => {
        logger.debug(s"Applying unnester rule C10")
        val (p_v, p_not_v) = p.partition(variables(_).map(_.idn) == Set(v.idn))
        CalculusTerm(CanonicalComp(m, r, p_not_v, e), u, PairPattern(w, IdnPattern(v)), AlgebraTerm(LogicalAlgebra.OuterUnnest(createExp(path, w), createPredicate(p_v, PairPattern(w, IdnPattern(v))), child)))
      }
    }

    /** Rule C11
      */

    /** Returns true if the comprehension `c` does not depend on `s` generators.
      */
    def areIndependent(c: Calculus.Comp, s: List[Calculus.Gen]) = {
      val sVs: Set[String] = s.map { case Calculus.Gen(Calculus.IdnDef(v, _), _) => v}.toSet
      variables(c).map(_.idn).intersect(sVs).isEmpty
    }

    /** Extractor object to pattern match nested comprehensions.
      */
    object NestedComp {

      import Calculus._

      def unapply(e: Exp): Option[Comp] = e match {
        case RecordProj(NestedComp(e), _)           => Some(e)
        case RecordCons(atts)                       => atts.map(att => att.e match {
          case NestedComp(e) => Some(e)
          case _             => None
        }).flatten.headOption
        case IfThenElse(NestedComp(e1), _, _)       => Some(e1)
        case IfThenElse(_, NestedComp(e2), _)       => Some(e2)
        case IfThenElse(_, _, NestedComp(e3))       => Some(e3)
        case BinaryExp(_, NestedComp(e1), _)        => Some(e1)
        case BinaryExp(_, _, NestedComp(e2))        => Some(e2)
        case ConsCollectionMonoid(_, NestedComp(e)) => Some(e)
        case MergeMonoid(_, NestedComp(e1), _)      => Some(e1)
        case MergeMonoid(_, _, NestedComp(e2))      => Some(e2)
        case UnaryExp(_, NestedComp(e))             => Some(e)
        case c: Comp                                => Some(c)
        case _                                      => None
      }
    }

    def hasNestedComp(ps: List[Calculus.Exp]) =
      ps.collectFirst { case NestedComp(c) => c}.isDefined

    def getNestedComp(ps: List[Calculus.Exp]) =
      ps.collectFirst { case NestedComp(c) => c}.head

    def freshIdn = SymbolTable.next()

    lazy val ruleC11 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, s, p, e1), u, w, child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) => {
        logger.debug(s"Applying unnester rule C11")
        val c = getNestedComp(p)
        val v = freshIdn
        val np = p.map(rewrite(attempt(oncetd(rule[Calculus.Exp] {
          case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
        })))(_))
        CalculusTerm(CanonicalComp(m, s, np, e1), u, PairPattern(w, IdnPattern(Calculus.IdnDef(v, None))), CalculusTerm(c, w, w, child))
      }
    }

    /** Rule C12
      */

    lazy val ruleC12 = rule[Term] {
      case CalculusTerm(CanonicalComp(m, Nil, p, f @ NestedComp(c)), u, w, child) => {
        logger.debug(s"Applying unnester rule C12")
        val v = freshIdn
        val nf = rewrite(oncetd(rule[Calculus.Exp] {
          case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
        }))(f)
        CalculusTerm(CanonicalComp(m, Nil, p, nf), u, PairPattern(w, IdnPattern(Calculus.IdnDef(v, None))), CalculusTerm(c, w, w, child))
      }
    }

    /** Extra Rule (not incl. in [1]) for handling top-level merge nodes
      */

    lazy val ruleTopLevelMerge = rule[Term] {
      case CalculusTerm(Calculus.MergeMonoid(m, e1, e2), _, _, _) => {
        logger.debug(s"Applying unnester rule TopLevelMerge")
        AlgebraTerm(LogicalAlgebra.Merge(m, unnest(e1), unnest(e2)))
      }
    }

    unnest(inTree.root match { case e: Calculus.Exp => e })
  }
}
