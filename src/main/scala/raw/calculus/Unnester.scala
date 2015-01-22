package raw.calculus

import com.typesafe.scalalogging.LazyLogging
import raw.RawException
import raw.algebra._

case class UnnesterError(err: String) extends RawException

/** Terms used during query unnesting.
  */
sealed abstract class Term

case object EmptyTerm extends Term

case class CalculusTerm(c: CanonicalCalculus.Exp, u: List[CanonicalCalculus.Var], w: List[CanonicalCalculus.Var], child: Term) extends Term

case class AlgebraTerm(t: LogicalAlgebra.AlgebraNode) extends Term

/** The query unnesting algorithm that converts a calculus expression (converted into its canonical form) into
  * a logical algebra plan.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
trait Unnester extends Simplifier with LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy

  def unnest(c: Calculus.Comp): LogicalAlgebra.AlgebraNode = unnestSimplified(simplify(c))

  private def unnestSimplified(e: CanonicalCalculus.Exp): LogicalAlgebra.AlgebraNode = {
    unnesterRules(CalculusTerm(e, List(), List(), EmptyTerm)) match {
      case Some(AlgebraTerm(a)) => a
      case e                    => throw UnnesterError(s"Invalid output expression: $e")
    }
  }

  // TODO: There must be a better way to define the strategy without relying explicitly on recursion.
  lazy val unnesterRules: Strategy =
    reduce(ruleC11 < unnesterRules + (ruleC12 < unnesterRules + (ruleC4 <+ ruleC5 <+ ruleC6 <+ ruleC7 <+ ruleC8 <+ ruleC9 <+ ruleC10 + ruleTopLevelMerge)))

  /** Return the set of variables used in an expression.
    */
  def variables(e: CanonicalCalculus.Exp): Set[CanonicalCalculus.Var] = {
    var vs = scala.collection.mutable.Set[CanonicalCalculus.Var]()
    everywhere(query[CanonicalCalculus.Exp] { case v: CanonicalCalculus.Var => vs += v})(e)
    vs.toSet
  }

  /** Split a list of predicates based on a list of variables.
    * The predicates which use *all* the variables are returned in the first list,
    * and the remaining are returned in the second list.
    */
  def splitPredicates(preds: List[CanonicalCalculus.Exp], vs: List[CanonicalCalculus.Var]): (List[CanonicalCalculus.Exp], List[CanonicalCalculus.Exp]) =
    preds.partition(p => variables(p) == vs.toSet)

  def convertVar(v: CanonicalCalculus.Var, vs: List[CanonicalCalculus.Var]): Arg =  Arg(vs.indexOf(v))

  /** Convert canonical calculus expression to algebra expression.
    * The position of each canonical expression variable is used as the argument.
    */
  def convertExp(e: CanonicalCalculus.Exp, vs: List[CanonicalCalculus.Var]): Exp = e match {
    case _: CanonicalCalculus.Null                    => Null
    case CanonicalCalculus.BoolConst(v)               => BoolConst(v)
    case CanonicalCalculus.IntConst(v)                => IntConst(v)
    case CanonicalCalculus.FloatConst(v)              => FloatConst(v)
    case CanonicalCalculus.StringConst(v)             => StringConst(v)
    case v: CanonicalCalculus.Var                     => convertVar(v, vs)
    case CanonicalCalculus.RecordProj(e, idn)         => RecordProj(convertExp(e, vs), idn)
    case CanonicalCalculus.RecordCons(atts)           => RecordCons(atts.map { att => AttrCons(att.idn, convertExp(att.e, vs))})
    case CanonicalCalculus.IfThenElse(e1, e2, e3)     => IfThenElse(convertExp(e1, vs), convertExp(e2, vs), convertExp(e3, vs))
    case CanonicalCalculus.BinaryExp(op, e1, e2)      => BinaryExp(op, convertExp(e1, vs), convertExp(e2, vs))
    case CanonicalCalculus.ZeroCollectionMonoid(m)    => ZeroCollectionMonoid(m)
    case CanonicalCalculus.ConsCollectionMonoid(m, e) => ConsCollectionMonoid(m, convertExp(e, vs))
    case CanonicalCalculus.MergeMonoid(m, e1, e2)     => MergeMonoid(m, convertExp(e1, vs), convertExp(e2, vs))
    case CanonicalCalculus.UnaryExp(op, e)            => UnaryExp(op, convertExp(e, vs))
    case _: CanonicalCalculus.Comp                    => throw UnnesterError(s"Unexpected comprehension: $e")
  }

  /** Convert canonical calculus path to algebra path.
    * As in `convertExp`, the position of each canonical expression variable is used as the argument.
    */
  def convertPath(p: CanonicalCalculus.Path, vs: List[CanonicalCalculus.Var]): Path = p match {
    case CanonicalCalculus.BoundVar(v)        => BoundArg(convertVar(v, vs))
    case CanonicalCalculus.ClassExtent(name)  => ClassExtent(name)
    case CanonicalCalculus.InnerPath(p, name) => InnerPath(convertPath(p, vs), name)
  }

  /** Rule C4
    */

  lazy val ruleC4 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, CanonicalCalculus.Gen(v, x: CanonicalCalculus.ClassExtent) :: r, p, e), Nil, Nil, EmptyTerm) => {
      logger.debug(s"Rule C4")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      CalculusTerm(CanonicalCalculus.Comp(m, r, p_not_v, e), Nil, List(v), AlgebraTerm(LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.name))))
    }
  }

  /** Rule C5
    */

  lazy val ruleC5 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, Nil, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C5")
      AlgebraTerm(LogicalAlgebra.Reduce(m, convertExp(e, w), p.map(convertExp(_, w)), child))
    }
  }

  /** Rule C6
    */

  lazy val ruleC6 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, CanonicalCalculus.Gen(v, x: CanonicalCalculus.ClassExtent) :: r, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C6")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      val (p_w_v, p_rest) = splitPredicates(p_not_v, w :+ v)
      CalculusTerm(CanonicalCalculus.Comp(m, r, p_rest, e), Nil, w :+ v, AlgebraTerm(LogicalAlgebra.Join(p_w_v.map(convertExp(_, w :+ v)), child, LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.name)))))
    }
  }

  /** Rule C7
    */

  lazy val ruleC7 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, CanonicalCalculus.Gen(v, path) :: r, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C7")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      CalculusTerm(CanonicalCalculus.Comp(m, r, p_not_v, e), Nil, w :+ v, AlgebraTerm(LogicalAlgebra.Unnest(convertPath(path, w), p_v.map(convertExp(_, w :+ v)), child)))
    }
  }

  /** Rule C8
    */

  lazy val ruleC8 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, Nil, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C8")
      AlgebraTerm(LogicalAlgebra.Nest(m, convertExp(e, w), u.map(convertVar(_, w)), p.map(convertExp(_, w)), w.filter(!u.contains(_)).map(convertVar(_, w)), child))
    }
  }

  /** Rule C9
    */

  lazy val ruleC9 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, CanonicalCalculus.Gen(v, x: CanonicalCalculus.ClassExtent) :: r, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C9")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      val (p_w_v, p_rest) = splitPredicates(p_not_v, w :+ v)
      CalculusTerm(CanonicalCalculus.Comp(m, r, p_rest, e), u, w :+ v, AlgebraTerm(LogicalAlgebra.OuterJoin(p_w_v.map(convertExp(_, w :+ v)), child, LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.name)))))
    }
  }

  /** Rule C10
    */

  lazy val ruleC10 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, CanonicalCalculus.Gen(v, path) :: r, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Rule C10")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      CalculusTerm(CanonicalCalculus.Comp(m, r, p_not_v, e), u, w :+ v, AlgebraTerm(LogicalAlgebra.OuterUnnest(convertPath(path, w), p_v.map(convertExp(_, w :+ v)), child)))
    }
  }

  /** Rule C11
    */

  /** Returns true if the comprehension `c` does not depend on `s` generators/
    */
  def areIndependent(c: CanonicalCalculus.Comp, s: List[CanonicalCalculus.Gen]) = {
    val sVs = s.map { case CanonicalCalculus.Gen(v, _) => v}.toSet
    variables(c).intersect(sVs).isEmpty
  }

  /** Extractor object to pattern match nested comprehensions. */
  private object NestedComp {

    import raw.calculus.CanonicalCalculus._

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

  def hasNestedComp(ps: List[CanonicalCalculus.Exp]) =
    ps.collectFirst { case NestedComp(c) => c}.isDefined

  def getNestedComp(ps: List[CanonicalCalculus.Exp]) =
    ps.collectFirst { case NestedComp(c) => c}.head

  // TODO: `hasNestedComp` followed by `getNestedComp` is very inefficient
  lazy val ruleC11 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, s, p, e1), u, w, child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) => {
      logger.debug(s"Rule C11")
      val c = getNestedComp(p)
      val v = CanonicalCalculus.Var()
      val np = p.map(rewrite(attempt(oncetd(rule[CanonicalCalculus.Exp] {
        case `c` => v
      })))(_))
      CalculusTerm(CanonicalCalculus.Comp(m, s, np, e1), u, w :+ v, CalculusTerm(c, w, w, child))
    }
  }

  /** Rule C12
    */

  lazy val ruleC12 = rule[Term] {
    case CalculusTerm(CanonicalCalculus.Comp(m, Nil, p, f@NestedComp(c)), u, w, child) => {
      logger.debug(s"Rule C12")
      val v = CanonicalCalculus.Var()
      val nf = rewrite(oncetd(rule[CanonicalCalculus.Exp] {
        case `c` => v
      }))(f)
      CalculusTerm(CanonicalCalculus.Comp(m, Nil, p, nf), u, w :+ v, CalculusTerm(c, w, w, child))
    }
  }

  /** Extra Rule (not incl. in [1]) for handling top-level merge nodes
    */

  lazy val ruleTopLevelMerge = rule[Term] {
    case CalculusTerm(CanonicalCalculus.MergeMonoid(m, e1, e2), _, _, _) => {
      logger.debug(s"rule TopLevelMerge")
      AlgebraTerm(LogicalAlgebra.Merge(m, unnestSimplified(e1), unnestSimplified(e2)))
    }

  }

}
