package raw
package calculus

case class NormalizerError(err: String) extends RawException(err)

/** Normalize a comprehension by transforming a tree into its normalized form.
  * The normalization rules are described in [1] (Fig. 4, page 17).
  */
trait Normalizer extends Uniquifier {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  override def strategy = attempt(super.strategy) <* normalize

  private lazy val normalize =
    doloop(
      reduce(rule1),
      oncetd(rule2 + rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10))

  /** Rule 1
    */

  private object Rule1 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b: Bind => b})
  }

  lazy val rule1 = rule[Comp] {
    case Comp(m, Rule1(r, Bind(PatternIdn(IdnDef(x)), u), s), e) =>
      logger.debug(s"Applying normalizer rule 1")
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => deepclone(u)
      })
      val ns = rewrite(strategy)(s)
      val ne = rewrite(strategy)(e)
      Comp(m, r ++ ns, ne)
  }

  /** Rule 2
    */

  private lazy val rule2 = rule[Exp] {
    case f @ FunApp(FunAbs(PatternIdn(IdnDef(idn)), e1), e2) =>
      logger.debug(s"Applying normalizer rule 2")
      rewrite(everywhere(rule[Exp] {
        case IdnExp(IdnUse(`idn`)) => deepclone(e2)
      }))(e1)
  }

  /** Rule 3
    */

  private lazy val rule3 = rule[Exp] {
    case r @ RecordProj(RecordCons(atts), idn) =>
      logger.debug(s"Applying normalizer rule 3")
      atts.collect { case att if att.idn == idn => att.e}.head
  }

  /** Rule 4
    */

  private object Rule4 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: IfThenElse) => g})
  }

  private lazy val rule4 = rule[Exp] {
    case Comp(m, Rule4(q, Gen(p, IfThenElse(e1, e2, e3)), s), e) if m.commutative.head || q.isEmpty =>  // TODO: Assuming the monoid is already defined
      logger.debug(s"Applying normalizer rule 4")
      val c1 = Comp(deepclone(m), q ++ Seq(e1, Gen(p, e2)) ++ s, e)
      val c2 = Comp(deepclone(m), q.map(deepclone) ++ Seq(UnaryExp(Not(), deepclone(e1)), Gen(deepclone(p), e3)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteIdns(c2))
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
    case Comp(m, Rule6(q, Gen(p, ConsCollectionMonoid(_, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 6")
      Comp(m, q ++ Seq(Bind(p, e1)) ++ s, e)
  }

  /** Rule 7
    */

  private object Rule7 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: MergeMonoid) => g})
  }

  private lazy val rule7 = rule[Exp] {
    case Comp(m, Rule7(q, Gen(p, MergeMonoid(_, e1, e2)), s), e) if m.commutative.head || q.isEmpty =>
      logger.debug(s"Applying normalizer rule 7")
      val c1 = Comp(deepclone(m), q ++ Seq(Gen(p, e1)) ++ s, e)
      val c2 = Comp(deepclone(m), q.map(deepclone) ++ Seq(Gen(deepclone(p), e2)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteIdns(c2))
  }

  /** Rule 8
    */

  private object Rule8 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: Comp) => g})
  }

  // TODO: Write testcae for this: two nested comprehensions on the Gen

  private lazy val rule8 = rule[Exp] {
    case Comp(m, Rule8(q, Gen(p, Comp(_, r, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 8")
      Comp(m, q ++ r ++ Seq(Bind(p, e1)) ++ s, e)
  }

  /** Rule 9
    */

  private object Rule9 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Comp](qs, { case c @ Comp(_: OrMonoid, _, _) => c})
  }

  private lazy val rule9 = rule[Exp] {
    case Comp(m, Rule9(q, Comp(_: OrMonoid, r, pred), s), e) if m.idempotent.head =>
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

object Normalizer {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree)
    val a = new SemanticAnalyzer(t1, world)
    val normalizer = new Normalizer {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(normalizer.strategy)(t1)
  }
}
