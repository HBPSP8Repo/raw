package raw.calculus

import raw._

/** The Normalization Algorithm for Monoid Comprehensions.
  *
  * The rules are described in [1] (Fig. 4, page 17).
  */
trait Normalizer extends SemanticAnalyzer {

  import org.kiama.attribution.AttributableSupport.deepclone
  import org.kiama.attribution.Attribution._
  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy
  import Calculus._

  /** Application of normalizer rules.
    *
    * Rule 1 and Rule 2 use the `entity` attribute, and therefore require a tree that is initialized
    * by Kiama, i.e. where each node's parent, child, siblings, etc mutable variables are properly set.
    * Since rules in general transform the tree, we do `initTree` before applying rules 1 and 2,
    * to ensure the tree they get is correct - i.e. to ensure that the `entity` attribute works as expected.
    *
    * For the remaining rules there is no need to call `initTree` in between rule applications,
    * since Kiama does NOT use those mutable properties to navigate through the tree.
    */
  def normalize(e: Exp): Exp = {
    val phase1 = applyRule1(e)
    val phase2 = applyRule2(phase1)
    val phase3 = applyReduce(rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10, phase2)
    if (phase2 == phase3) phase3 else normalize(phase3)
  }

  def applyRule1(e: Exp) = applyOnceTopDown(rule1, e)

  def applyRule2(e: Exp) = applyOnceTopDown(rule2, e)

  def applyOnceTopDown(r: Strategy, e: Exp): Exp = {
    initTree(e)
    oncetd(r)(e) match {
      case Some(ne: Exp) => applyOnceTopDown(r, ne)
      case _             => e
    }
  }

  def applyReduce(r: Strategy, e: Exp): Exp = reduce(r)(e) match {
    case Some(ne: Exp) => ne
    case _             => e
  }

  /** Rule 1: Beta reduction for Bind
    */
  object Rule1 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b: Bind => b})
  }

  lazy val rule1 = rule[Comp] {
    case c@Comp(m, Rule1(r, Bind(x, u), s), e) =>
      val ns = s.map {
        case e: Exp       => subst(e, x, u)
        case Bind(idn, e) => Bind(idn, subst(e, x, u))
        case Gen(idn, e)  => Gen(idn, subst(e, x, u))
      }
      Comp(m, r ++ ns, subst(e, x, u));
  }

  /** Replace x by u in e. */
  def subst(e: Exp, x: IdnNode, u: Exp): Exp = e match {
    case IdnExp(idn) if (idn -> entity) eq (x -> entity) => deepclone(u)
    case RecordProj(e, idn)                              => RecordProj(subst(e, x, u), idn)
    case RecordCons(atts)                                => RecordCons(atts.map(att => AttrCons(att.idn, subst(att.e, x, u))))
    case IfThenElse(e1, e2, e3)                          => IfThenElse(subst(e1, x, u), subst(e2, x, u), subst(e3, x, u))
    case BinaryExp(op, e1, e2)                           => BinaryExp(op, subst(e1, x, u), subst(e2, x, u))
    case FunApp(f, e)                                    => FunApp(subst(f, x, u), subst(e, x, u))
    case ConsCollectionMonoid(m, e)                      => ConsCollectionMonoid(m, subst(e, x, u))
    case MergeMonoid(m, e1, e2)                          => MergeMonoid(m, subst(e1, x, u), subst(e2, x, u))
    case Comp(m, qs, e)                                  =>
      val nqs = qs.map {
        case e: Exp       => subst(e, x, u)
        case Bind(idn, e) => Bind(idn, subst(e, x, u))
        case Gen(idn, e)  => Gen(idn, subst(e, x, u))
      }
      Comp(m, nqs, subst(e, x, u))
    case UnaryExp(op, e)                                 => UnaryExp(op, subst(e, x, u))
    case FunAbs(idn, t, e)                               => FunAbs(idn, t, subst(e, x, u))
    case e                                               => e
  }

  /** Splits a list using a partial function.
    */
  def splitWith[A, B](xs: Seq[A], f: PartialFunction[A, B]): Option[Tuple3[Seq[A], B, Seq[A]]] = {
    val begin = xs.takeWhile(x => if (!f.isDefinedAt(x)) true else false)
    if (begin.length == xs.length) {
      None
    } else {
      val elem = f(xs(begin.length))
      if (begin.length + 1 == xs.length) {
        Some((begin, elem, Seq()))
      } else {
        Some(begin, elem, xs.takeRight(xs.length - begin.length - 1))
      }
    }
  }

  /** Rule 2: Beta reduction for Function Application
    */
  lazy val rule2 = rule[Exp] {
    case f@FunApp(FunAbs(idn, _, e1), e2) => subst(e1, idn, e2)
  }

  /** Rule 3: Project a record
    */
  lazy val rule3 = rule[Exp] {
    case r@RecordProj(RecordCons(atts), idn) => atts.collect { case att if att.idn == idn => att.e}.head
  }

  /** Rule 4: ...
    */

  object Rule4 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g@Gen(_, _: IfThenElse) => g})
  }

  lazy val rule4 = rule[Exp] {
    case Comp(m, Rule4(q, Gen(idn, IfThenElse(e1, e2, e3)), s), e) if m.commutative || q.isEmpty =>
      MergeMonoid(m,
        Comp(m, q ++ Seq(e1, Gen(idn, e2)) ++ s, e),
        Comp(m, q ++ Seq(UnaryExp(Not(), deepclone(e1)), Gen(deepclone(idn), e3)) ++ s.map(deepclone(_)), deepclone(e)))

  }

  /** Rule 5: ...
    */

  object Rule5 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g@Gen(_, _: ZeroCollectionMonoid) => g})
  }

  lazy val rule5 = rule[Exp] {
    case Comp(m, Rule5(q, Gen(idn, ze: ZeroCollectionMonoid), s), e) => m match {
      // TODO: Add -infinity
      case _: MaxMonoid        => Calculus.IntConst(0)
      case _: MultiplyMonoid   => Calculus.IntConst(1)
      case _: SumMonoid        => Calculus.IntConst(0)
      case _: AndMonoid        => Calculus.BoolConst(true)
      case _: OrMonoid         => Calculus.BoolConst(false)
      case m: CollectionMonoid => Calculus.ZeroCollectionMonoid(m)
    }
  }

  /** Rule 6: ...
    */

  object Rule6 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g@Gen(_, _: ConsCollectionMonoid) => g})
  }

  lazy val rule6 = rule[Exp] {
    case Comp(m, Rule6(q, Gen(idn, ConsCollectionMonoid(_, e1)), s), e) =>
      Comp(m, q ++ Seq(Bind(idn, e1)) ++ s, e)
  }

  /** Rule 7: ...
    */

  object Rule7 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g@Gen(_, _: MergeMonoid) => g})
  }

  lazy val rule7 = rule[Exp] {
    case Comp(m, Rule7(q, Gen(idn, MergeMonoid(_, e1, e2)), s), e) if m.commutative || q.isEmpty =>
      MergeMonoid(m,
        Comp(deepclone(m), q ++ Seq(Gen(idn, e1)) ++ s, e),
        Comp(deepclone(m), q.map(deepclone(_)) ++ Seq(Gen(deepclone(idn), e2)) ++ s.map(deepclone(_)), deepclone(e)))
  }

  /** Rule 8: ...
    */

  object Rule8 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g@Gen(_, _: Comp) => g})
  }

  lazy val rule8 = rule[Exp] {
    case Comp(m, Rule8(q, Gen(idn, Comp(_, r, e1)), s), e) =>
      // TODO: Is there an issue with the entity of `idn` changing from being a Generator to a Variable?
      // TODO: If `idn` is cloned, then all its uses in `e1` should be cloned as well.
      Comp(m, q ++ r ++ Seq(Bind(idn, e1)) ++ s, e)
  }

  /** Rule 9: ...
    */

  object Rule9 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Comp](qs, { case c@Comp(_: OrMonoid, _, _) => c})
  }

  lazy val rule9 = rule[Exp] {
    case Comp(m, Rule9(q, Comp(_: OrMonoid, r, pred), s), e) if m.idempotent =>
      Comp(m, q ++ r ++ Seq(pred) ++ s, e)
  }

  /** Rule 10: ...
    */

  lazy val rule10 = rule[Exp] {
    case Comp(m: PrimitiveMonoid, s, Comp(m1, r, e)) if m == m1 =>
      Comp(m, s ++ r, e)
  }
}
