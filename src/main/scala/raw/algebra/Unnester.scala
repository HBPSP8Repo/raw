package raw
package algebra

import com.typesafe.scalalogging.LazyLogging
import calculus.{Calculus, SymbolTable}

case class UnnesterError(err: String) extends RawException

/** Terms used during query unnesting.
  */
sealed abstract class Term
case object EmptyTerm extends Term
case class CalculusTerm(c: Calculus.Exp, u: List[String], w: List[String], child: Term) extends Term
case class AlgebraTerm(t: LogicalAlgebra.AlgebraNode) extends Term

/** Algorithm that converts a calculus expression (in canonical form) into the logical algebra.
  * The algorithm is described in Fig. 10 of [1], page 34.
  */
object Unnester extends LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy

  def apply(tree: Calculus.Calculus): LogicalAlgebra.AlgebraNode = unnest(tree.root)

  private def unnest(e: Calculus.Exp): LogicalAlgebra.AlgebraNode = {
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
  def variables(e: Calculus.Exp): Set[String] = {
    var vs = scala.collection.mutable.Set[String]()
    everywhere(query[Calculus.Exp] { case Calculus.IdnExp(idn) => vs += idn.idn})(e)
    vs.toSet
  }

  def convertVar(v: String, vs: List[String]): Arg = Arg(vs.indexOf(v))

  /** Convert canonical calculus expression to algebra expression.
    * The position of each canonical expression variable is used as the argument.
    */
  def convertExp(e: Calculus.Exp, vs: List[String]): Exp = e match {
    case _: Calculus.Null                    => Null
    case Calculus.BoolConst(v)               => BoolConst(v)
    case Calculus.IntConst(v)                => IntConst(v)
    case Calculus.FloatConst(v)              => FloatConst(v)
    case Calculus.StringConst(v)             => StringConst(v)
    case Calculus.IdnExp(idn)                => convertVar(idn.idn, vs)
    case Calculus.RecordProj(e, idn)         => RecordProj(convertExp(e, vs), idn)
    case Calculus.RecordCons(atts)           => RecordCons(atts.map { att => AttrCons(att.idn, convertExp(att.e, vs))})
    case Calculus.IfThenElse(e1, e2, e3)     => IfThenElse(convertExp(e1, vs), convertExp(e2, vs), convertExp(e3, vs))
    case Calculus.BinaryExp(op, e1, e2)      => BinaryExp(op, convertExp(e1, vs), convertExp(e2, vs))
    case Calculus.ZeroCollectionMonoid(m)    => ZeroCollectionMonoid(m)
    case Calculus.ConsCollectionMonoid(m, e) => ConsCollectionMonoid(m, convertExp(e, vs))
    case Calculus.MergeMonoid(m, e1, e2)     => MergeMonoid(m, convertExp(e1, vs), convertExp(e2, vs))
    case Calculus.UnaryExp(op, e)            => UnaryExp(op, convertExp(e, vs))
    case n                                   => throw UnnesterError(s"Unexpected node: $n")
  }

  /** Convert canonical calculus path to algebra path.
    * As in `convertExp`, the position of each canonical expression variable is used as the argument.
    */
  def convertPath(p: Calculus.Path, vs: List[String]): Path = p match {
    case Calculus.BoundPath(v)       => BoundArg(convertVar(v.idn, vs))
    case Calculus.ClassExtent(idn)   => ClassExtent(idn)
    case Calculus.InnerPath(p, idn)  => InnerPath(convertPath(p, vs), idn)
  }

  /** Rule C4
    */

  lazy val ruleC4 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Calculus.GenPath(Calculus.IdnDef(v), x: Calculus.ClassExtent) :: r, p, e), Nil, Nil, EmptyTerm) => {
      logger.debug(s"Applying unnester rule C4")
      val (p_v, p_not_v) = p.partition(variables(_) == Set(v))
      CalculusTerm(Calculus.CanonicalComp(m, r, p_not_v, e), Nil, List(v), AlgebraTerm(LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.idn))))
    }
  }

  /** Rule C5
    */

  lazy val ruleC5 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Nil, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C5")
      AlgebraTerm(LogicalAlgebra.Reduce(m, convertExp(e, w), p.map(convertExp(_, w)), child))
    }
  }

  /** Rule C6
    */

  lazy val ruleC6 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Calculus.GenPath(Calculus.IdnDef(v), x: Calculus.ClassExtent) :: r, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C6")
      val p_v = p.filter(variables(_) == Set(v))
      val p_w_v = p.filter(pred => (w :+ v).toSet.subsetOf(variables(pred)))
      val p_rest = p.filter(pred => !p_v.contains(pred) && !p_w_v.contains(pred))
      CalculusTerm(Calculus.CanonicalComp(m, r, p_rest, e), Nil, w :+ v, AlgebraTerm(LogicalAlgebra.Join(p_w_v.map(convertExp(_, w :+ v)), child, LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.idn)))))
    }
  }

  /** Rule C7
    */

  lazy val ruleC7 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Calculus.GenPath(Calculus.IdnDef(v), path) :: r, p, e), Nil, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C7")
      val (p_v, p_not_v) = p.partition(variables(_) == Set(v))
      CalculusTerm(Calculus.CanonicalComp(m, r, p_not_v, e), Nil, w :+ v, AlgebraTerm(LogicalAlgebra.Unnest(convertPath(path, w), p_v.map(convertExp(_, w :+ v)), child)))
    }
  }

  /** Rule C8
    */

  lazy val ruleC8 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Nil, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C8")
      AlgebraTerm(LogicalAlgebra.Nest(m, convertExp(e, w), u.map(convertVar(_, w)), p.map(convertExp(_, w)), w.filter(!u.contains(_)).map(convertVar(_, w)), child))
    }
  }

  /** Rule C9
    */

  lazy val ruleC9 = rule[Term] {
    case foo @ CalculusTerm(Calculus.CanonicalComp(m, Calculus.GenPath(Calculus.IdnDef(v), x: Calculus.ClassExtent) :: r, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C9: $foo")
      val p_v = p.filter(variables(_) == Set(v))
      val p_w_v = p.filter(pred => (w :+ v).toSet.subsetOf(variables(pred)))
      val p_rest = p.filter(pred => !p_v.contains(pred) && !p_w_v.contains(pred))
      CalculusTerm(Calculus.CanonicalComp(m, r, p_rest, e), u, w :+ v, AlgebraTerm(LogicalAlgebra.OuterJoin(p_w_v.map(convertExp(_, w :+ v)), child, LogicalAlgebra.Select(p_v.map(convertExp(_, List(v))), LogicalAlgebra.Scan(x.idn)))))
    }
  }

  /** Rule C10
    */

  lazy val ruleC10 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Calculus.GenPath(Calculus.IdnDef(v), path) :: r, p, e), u, w, AlgebraTerm(child)) => {
      logger.debug(s"Applying unnester rule C10")
      val (p_v, p_not_v) = p.partition(variables(_) == Set(v))
      CalculusTerm(Calculus.CanonicalComp(m, r, p_not_v, e), u, w :+ v, AlgebraTerm(LogicalAlgebra.OuterUnnest(convertPath(path, w), p_v.map(convertExp(_, w :+ v)), child)))
    }
  }

  /** Rule C11
    */

  /** Returns true if the comprehension `c` does not depend on `s` generators/
    */
  def areIndependent(c: Calculus.CanonicalComp, s: List[Calculus.GenPath]) = {
    val sVs: Set[String] = s.map { case Calculus.GenPath(Calculus.IdnDef(v), _) => v}.toSet
    variables(c).intersect(sVs).isEmpty
  }

  /** Extractor object to pattern match nested comprehensions.
    */
  private object NestedComp {
    def unapply(e: Calculus.Exp): Option[Calculus.CanonicalComp] = e match {
      case Calculus.RecordProj(NestedComp(e), _)           => Some(e)
      case Calculus.RecordCons(atts) => atts.map(att => att.e match {
        case NestedComp(e) => Some(e)
        case _             => None
      }).flatten.headOption
      case Calculus.IfThenElse(NestedComp(e1), _, _)       => Some(e1)
      case Calculus.IfThenElse(_, NestedComp(e2), _)       => Some(e2)
      case Calculus.IfThenElse(_, _, NestedComp(e3))       => Some(e3)
      case Calculus.BinaryExp(_, NestedComp(e1), _)        => Some(e1)
      case Calculus.BinaryExp(_, _, NestedComp(e2))        => Some(e2)
      case Calculus.ConsCollectionMonoid(_, NestedComp(e)) => Some(e)
      case Calculus.MergeMonoid(_, NestedComp(e1), _)      => Some(e1)
      case Calculus.MergeMonoid(_, _, NestedComp(e2))      => Some(e2)
      case Calculus.UnaryExp(_, NestedComp(e))             => Some(e)
      case c: Calculus.CanonicalComp                       => Some(c)
      case _                                               => None
    }
  }

  def hasNestedComp(ps: List[Calculus.Exp]) =
    ps.collectFirst { case NestedComp(c) => c}.isDefined

  def getNestedComp(ps: List[Calculus.Exp]) =
    ps.collectFirst { case NestedComp(c) => c}.head

  def freshVar = SymbolTable.next()

  // TODO: `hasNestedComp` followed by `getNestedComp` is very inefficient
  lazy val ruleC11 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, s, p, e1), u, w, child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) => {
      logger.debug(s"Applying unnester rule C11")
      val c = getNestedComp(p)
      val v = freshVar
      val np = p.map(rewrite(attempt(oncetd(rule[Calculus.Exp] {
        case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
      })))(_))
      CalculusTerm(Calculus.CanonicalComp(m, s, np, e1), u, w :+ v, CalculusTerm(c, w, w, child))
    }
  }

  /** Rule C12
    */

  lazy val ruleC12 = rule[Term] {
    case CalculusTerm(Calculus.CanonicalComp(m, Nil, p, f @ NestedComp(c)), u, w, child) => {
      logger.debug(s"Applying unnester rule C12")
      val v = freshVar
      val nf = rewrite(oncetd(rule[Calculus.Exp] {
        case `c` => Calculus.IdnExp(Calculus.IdnUse(v))
      }))(f)
      CalculusTerm(Calculus.CanonicalComp(m, Nil, p, nf), u, w :+ v, CalculusTerm(c, w, w, child))
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

}