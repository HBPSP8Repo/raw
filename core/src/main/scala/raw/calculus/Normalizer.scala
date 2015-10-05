package raw
package calculus

/** Normalize a comprehension by transforming a tree into its normalized form.
  * The normalization rules are described in [1] (Fig. 4, page 17).
  */
class Normalizer extends PipelinedTransformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = normalize

  private lazy val normalize =
    doloop(
      reduce(rule1),
      oncetd(rule2 + rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10))

  // TODO: Move this into Monoids.scala (take it out from SemanticAnalyzer.scala as well)
  private def commutative(m: Monoid): Option[Boolean] = m match {
    case _: PrimitiveMonoid => Some(true)
    case _: SetMonoid => Some(true)
    case _: BagMonoid => Some(true)
    case _: ListMonoid => Some(false)
    case GenericMonoid(c, _, _) => c
  }

  private def idempotent(m: Monoid): Option[Boolean] = m match {
    case _: MaxMonoid => Some(true)
    case _: MinMonoid => Some(true)
    case _: MultiplyMonoid => Some(false)
    case _: SumMonoid => Some(false)
    case _: AndMonoid => Some(true)
    case _: OrMonoid => Some(true)
    case _: SetMonoid => Some(true)
    case _: BagMonoid => Some(false)
    case _: ListMonoid => Some(false)
    case GenericMonoid(_, i, _) => i
  }

  /** Rule 1
    */

  private object Rule1 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b: Bind => b})
  }

  lazy val rule1 = rule[Comp] {
    case n @ Comp(m, Rule1(r, Bind(PatternIdn(IdnDef(x)), u), s), e) =>
      logger.debug(s"Applying normalizer rule 1 to ${CalculusPrettyPrinter(n)}")
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => rewriteInternalIdns(deepclone(u))
      })
      val ns = rewrite(strategy)(s)
      val ne = rewrite(strategy)(e)
      Comp(m, r ++ ns, ne)
  }

  /** Rule 2
    */

  private lazy val rule2 = rule[Exp] {
    case n @ FunApp(FunAbs(PatternIdn(IdnDef(idn)), e1), e2) =>
      logger.debug(s"Applying normalizer rule 2 to ${CalculusPrettyPrinter(n)}")
      rewrite(everywhere(rule[Exp] {
        case IdnExp(IdnUse(`idn`)) => rewriteInternalIdns(deepclone(e2))
      }))(e1)
  }

  /** Rule 3
    */

  private lazy val rule3 = rule[Exp] {
    case n @ RecordProj(RecordCons(atts), idn) =>
      logger.debug(s"Applying normalizer rule 3 to ${CalculusPrettyPrinter(n)}")
      atts.collect { case att if att.idn == idn => att.e}.head
  }

  /** Rule 4
    */

  private object Rule4 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: IfThenElse) => g})
  }

  private lazy val rule4 = rule[Exp] {
    case Comp(m, Rule4(q, Gen(p, IfThenElse(e1, e2, e3)), s), e) if commutative(m).head || q.isEmpty =>  // TODO: Assuming the monoid is already defined
      logger.debug(s"Applying normalizer rule 4")
      val c1 = Comp(deepclone(m), q ++ Seq(e1, Gen(p, e2)) ++ s, e)
      val c2 = Comp(deepclone(m), q.map(deepclone) ++ Seq(UnaryExp(Not(), deepclone(e1)), Gen(deepclone(p), e3)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteInternalIdns(c2))
  }

  /** Rule 5
    */

  private object Rule5 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ZeroCollectionMonoid) => g})
  }

  private lazy val rule5 = rule[Exp] {
    case Comp(m, Rule5(q, Gen(_, ze: ZeroCollectionMonoid), s), e) =>
      logger.debug(s"Applying normalizer rule 5")
      m match {
        case _: MaxMonoid        => IntConst("0")
        case _: MultiplyMonoid   => IntConst("1")
        case _: SumMonoid        => IntConst("0")
        case _: AndMonoid        => BoolConst(true)
        case _: OrMonoid         => BoolConst(false)
        case m: CollectionMonoid => ZeroCollectionMonoid(m)
      }
  }

  /** Rule 6
    */

  private object Rule6 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ConsCollectionMonoid) => g})
  }

  private lazy val rule6 = rule[Exp] {
    case Comp(m, Rule6(q, Gen(Some(p), ConsCollectionMonoid(_, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 6")
      Comp(m, q ++ Seq(Bind(p, e1)) ++ s, e)
  }

  /** Rule 7
    */

  private object Rule7 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: MergeMonoid) => g})
  }

  private lazy val rule7 = rule[Exp] {
    case Comp(m, Rule7(q, Gen(p, MergeMonoid(_, e1, e2)), s), e) if commutative(m).head || q.isEmpty =>
      logger.debug(s"Applying normalizer rule 7")
      val c1 = Comp(deepclone(m), q ++ Seq(Gen(p, e1)) ++ s, e)
      val c2 = Comp(deepclone(m), q.map(deepclone) ++ Seq(Gen(deepclone(p), e2)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteInternalIdns(c2))
  }

  /** Rule 8
    */

  private object Rule8 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: Comp) => g})
  }

  private lazy val rule8 = rule[Exp] {
    case n @ Comp(m, Rule8(q, Gen(Some(p), Comp(_, r, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 8 to ${CalculusPrettyPrinter(n)}")
      val n1 = Comp(m, q ++ r ++ Seq(Bind(p, e1)) ++ s, e)
      logger.debug(s"Output is ${CalculusPrettyPrinter(n1)}")
      n1
  }

  /** Rule 9
    */

  private object Rule9 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Comp](qs, { case c @ Comp(_: OrMonoid, _, _) => c})
  }

  private lazy val rule9 = rule[Exp] {
    case Comp(m, Rule9(q, Comp(_: OrMonoid, r, pred), s), e) if idempotent(m).head =>
      logger.debug(s"Applying normalizer rule 9")
      Comp(m, q ++ r ++ Seq(pred) ++ s, e)
  }

  /** Rule 10
    */

  private lazy val rule10 = rule[Exp] {
    case Comp(m: PrimitiveMonoid, s, Comp(m1, r, e)) if m == m1 =>
      logger.debug(s"Applying normalizer rule 10")
      Comp(m, s ++ r, e)
  }

}
