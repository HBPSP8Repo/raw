package raw.calculus

import raw.calculus.CanonicalCalculus._
import raw.algebra._

sealed abstract class Term
case object EmptyTerm extends Term
case class Box(comp: Comp, u: List[Var], w: List[Var], child: Term) extends Term
case class Algebra(t: AlgebraNode) extends Term

trait Unnester extends Canonizer {

  import org.kiama.rewriting.Strategy
  import org.kiama.rewriting.Rewriter._

  def unnest(c: Calculus.Comp): AlgebraNode = {
    def apply(t: Term): Term = unnesterRules(t) match {
        case Some(nt: Algebra) => nt
        case Some(nt: Box) => apply(nt)
        case None => t
      }

    apply(Box(canonize(c), List(), List(), EmptyTerm)) match {
      case Algebra(a) => a
    }
  }

  lazy val unnesterRules: Strategy =
    // TODO: Replace recursive call by a RULE that re-applies itself, until it no longer applies.
    reduce(ruleC11 < unnesterRules + (ruleC12 < unnesterRules + (ruleC4 <+ ruleC5 <+ ruleC6 <+ ruleC7 <+ ruleC8 <+ ruleC9 <+ ruleC10)))

  /** Return the set of variables used in an expression.
    */
  def variables(e: Exp): Set[Var] = {
    var vars = scala.collection.mutable.Set[Var]()
    everywhere(query[Exp] { case v: Var => vars += v })(e)
    vars.toSet
  }

  /** Split a list of predicates based on a list of variables.
    * The predicates which use *all* the variables are returned in the first list,
    * and the remaining are returned in the second list.
    */
  def splitPredicates(preds: List[Exp], vars: List[Var]): (List[Exp], List[Exp]) =
    preds.partition(p => variables(p) == vars.toSet)

  /** Rule C4
    */

  lazy val ruleC4 = rule[Term] {
    case Box(Comp(m, Gen(v, x: ClassExtent) :: r, p, e), Nil, Nil, EmptyTerm) => {
      println("C4")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      val qq = Box(Comp(m, r, p_not_v, e), Nil, List(v), Algebra(Select(p_v, Scan(x.name))))
      println("qq 4 is " + qq)
      qq
    }
  }

  /** Rule C5
   */

  lazy val ruleC5 = rule[Term] {
    case Box(Comp(m, Nil, p, e), Nil, w, Algebra(child)) => println("C5");
      Algebra(Reduce(m, e, p, child))
  }

  /** Rule C6
   */

  lazy val ruleC6 = rule[Term] {
    case Box(Comp(m, Gen(v, x: ClassExtent) :: r, p, e), Nil, w, Algebra(child)) => {
      println("C6")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      val (p_w_v, p_rest) = splitPredicates(p_not_v, w :+ v)
      Box(Comp(m, r, p_rest, e), Nil, w :+ v, Algebra(Join(p_w_v, child, Select(p_v, Scan(x.name)))))
    }
  }

  /** Rule C7
   */

  lazy val ruleC7 = rule[Term] {
    case Box(Comp(m, Gen(v, path) :: r, p, e), Nil, w, Algebra(child)) => {
      println("C7")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      Box(Comp(m, r, p_not_v, e), Nil, w :+ v, Algebra(Unnest(path, p_v, child)))
    }
  }

  /** Rule C8
   */

  lazy val ruleC8 = rule[Term] {
    case Box(Comp(m, Nil, p, e), u, w, Algebra(child)) => println("C8");
      Algebra(Nest(m, e, u, p, w.filter(!u.contains(_)), child))
  }

  /** Rule C9
   */

  lazy val ruleC9 = rule[Term] {
    case Box(Comp(m, Gen(v, x: ClassExtent) :: r, p, e), u, w, Algebra(child)) => {
      println("C9")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      val (p_w_v, p_rest) = splitPredicates(p_not_v, w :+ v)
      Box(Comp(m, r, p_rest, e), u, w :+ v, Algebra(OuterJoin(p_w_v, child, Select(p_v, Scan(x.name)))))
    }
  }

  /** Rule C10
   */

  lazy val ruleC10 = rule[Term] {
    case Box(Comp(m, Gen(v, path) :: r, p, e), u, w, Algebra(child)) => {
      println("C10")
      val (p_v, p_not_v) = splitPredicates(p, List(v))
      println("here with v " +p_v + " and not v " + p_not_v)
      val qq = Box(Comp(m, r, p_not_v, e), u, w :+ v, Algebra(OuterUnnest(path, p_v, child)))
      println("qq 10 is " + qq)
      qq
    }
  }

  /** Rule C11
   */

  /** Returns true if the comprehension `c` does not depend on `s` generators/
   */
  def areIndependent(c: Comp, s: List[Gen]) = {
    val sVars = s.map { case Gen(v, _) => v }.toSet
    variables(c).intersect(sVars).isEmpty
  }

  /** Helper to pattern match the first comprehension in a list of expressions.
   */

  /** Helper object to pattern match nested comprehensions. */
  private object NestedComp {
    def unapply(e: Exp): Option[Comp] = e match {
      case RecordProj(NestedComp(e), _) => Some(e)
      case RecordCons(atts) => atts.map(att => att.e match {
        case NestedComp(e) => Some(e)
        case _ => None
      }).flatten.headOption
      case IfThenElse(NestedComp(e1), _, _) => Some(e1)
      case IfThenElse(_, NestedComp(e2), _) => Some(e2)
      case IfThenElse(_, _, NestedComp(e3)) => Some(e3)
      case BinaryExp(_, NestedComp(e1), _) => Some(e1)
      case BinaryExp(_, _, NestedComp(e2)) => Some(e2)
      case ConsCollectionMonoid(_, NestedComp(e)) => Some(e)
      case MergeMonoid(_, NestedComp(e1), _) => Some(e1)
      case MergeMonoid(_, _, NestedComp(e2)) => Some(e2)
      case UnaryExp(_, NestedComp(e)) => Some(e)
      case c: Comp => Some(c)
      case _ => None
    }
  }

  def hasNestedComp(ps: List[Exp]) =
    ps.collectFirst{ case NestedComp(c) => c }.isDefined

  def getNestedComp(ps: List[Exp]) =
    ps.collectFirst{ case NestedComp(c) => c }.head

  // TODO: Fix!!!
  lazy val ruleC11 = rule[Term] {
    case Box(Comp(m, s, p, e1), u, w, child) if hasNestedComp(p) && areIndependent(getNestedComp(p), s) => {
      val c = getNestedComp(p)
      println("C11")
      val v = Var()
      val np = p.map(rewrite(attempt(oncetd(rule[Exp] {
        case c1 if c1 == c => v
      })))(_))
      Box(Comp(m, s, np, e1), u, w :+ v, Box(c, w, w, child))
    }
  }

  /** Rule C12
   */

  lazy val ruleC12 = rule[Term] {
    case Box(Comp(m, Nil, p, f @ NestedComp(c)), u, w, child) =>
      println("C12")
      val v = Var()
      val nf = rewrite(oncetd(rule[Exp] {
        case c1 if c1 == c => v
      }))(f)
      println("f is " + f + " and nf is " + nf + " and c is " + c + " and f is " + f)
      val qq = Box(Comp(m, Nil, p, nf), u, w :+ v, Box(c, w, w, child))
      println("qq 12 is " + qq)
      qq
  }

}
