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

  override def strategy = super.strategy <* desugar <* normalize

  lazy val desugar = reduce(rulePatternFunAbs + rulePatternGen + rulePatternBindExpBlock + rulePatternBindComp +
    ruleExpBlocks + ruleEmptyExpBlock)

  lazy val normalize = doloop(reduce(rule1), oncetd(rule2 + rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10))

  /** Splits a list using a partial function. */
  private def splitWith[A, B](xs: Seq[A], f: PartialFunction[A, B]): Option[Tuple3[Seq[A], B, Seq[A]]] = {
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

  /** De-sugar pattern function abstractions into expression blocks.
    * e.g. `\((a,b),c) -> a + b + c` becomes `\x -> { (a,b) := x._1; c := x._2; a + b + c }`
    */

  lazy val rulePatternFunAbs = rule[Exp] {
    case PatternFunAbs(p @ PatternProd(ps), e) =>
      val idn = SymbolTable.next()
      FunAbs(IdnDef(idn, None),
        ExpBlock(
          ps.zipWithIndex.map{
            case (p1, idx) => p1 match {
              case PatternIdn(idn1) => Bind(idn1, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
              case p2: PatternProd  => PatternBind(p2, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
            }
          }, e))
  }

  /** De-sugar pattern generators.
    * e.g. `for ( ((a,b),c) <- X, ...` becomes `for (x <- X, ((a,b),c) := x, ...)`
    */

  object RulePatternGen {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternGen](qs, { case g: PatternGen => g})
  }

  lazy val rulePatternGen = rule[Comp] {
    case Comp(m, RulePatternGen(r, PatternGen(p, u), s), e) =>
      val idn = SymbolTable.next()
      Comp(m, r ++ Seq(Gen(IdnDef(idn, None), u), PatternBind(p, IdnExp(IdnUse(idn)))) ++ s, e)
  }

  /** De-sugar pattern binds inside expression blocks.
    * e.g. `{ ((a,b),c) = X; ... }` becomes `{ (a,b) = X._1; c = X._2; ... }`
    */

  lazy val rulePatternBindExpBlock = rule[ExpBlock] {
    case ExpBlock(PatternBind(PatternProd(ps), u) :: rest, e) =>
      ExpBlock(ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(u, s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(u, s"_${idx + 1}"))
        }
      } ++ rest, e)
  }

  /** De-sugar pattern binds inside comprehension qualifiers.
    * e.g. `for (..., ((a,b),c) = X, ...)` becomes `for (..., (a,b) = X._1, c = X._2, ...)`
    */

  object RulePatternBindComp {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternBind](qs, { case b: PatternBind => b})
  }

  lazy val rulePatternBindComp = rule[Comp] {
    case Comp(m, RulePatternBindComp(r, PatternBind(PatternProd(ps), u), s), e) =>
      Comp(m, r ++ ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(u, s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(u, s"_${idx + 1}"))
        }
      } ++ s, e)
  }

  /** De-sugar expression blocks by removing the binds one-at-a-time.
    */

  lazy val ruleExpBlocks = rule[ExpBlock] {
    case ExpBlock(Bind(IdnDef(x, _), u) :: rest, e) =>
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => deepclone(u)
      })
      val nrest = rewrite(strategy)(rest)
      val ne = rewrite(strategy)(e)
      ExpBlock(nrest, ne)
  }

  /** De-sugar expression blocks without bind statements.
    */

  lazy val ruleEmptyExpBlock = rule[Exp] {
    case ExpBlock(Nil, e) => e
  }

  /** Rule 1
    */

  object Rule1 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b: Bind => b})
  }

  lazy val rule1 = rule[Comp] {
    case Comp(m, Rule1(r, Bind(IdnDef(x, _), u), s), e) =>
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

  lazy val rule2 = rule[Exp] {
    case f @ FunApp(FunAbs(IdnDef(idn, _), e1), e2) =>
      logger.debug(s"Applying normalizer rule 2")
      rewrite(everywhere(rule[Exp] {
        case IdnExp(IdnUse(`idn`)) => deepclone(e2)
      }))(e1)
  }

  /** Rule 3
    */

  lazy val rule3 = rule[Exp] {
    case r @ RecordProj(RecordCons(atts), idn) =>
      logger.debug(s"Applying normalizer rule 3")
      atts.collect { case att if att.idn == idn => att.e}.head
  }

  /** Rule 4
    */

  object Rule4 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: IfThenElse) => g})
  }

  lazy val rule4 = rule[Exp] {
    case Comp(m, Rule4(q, Gen(idn, IfThenElse(e1, e2, e3)), s), e) if m.commutative || q.isEmpty =>
      logger.debug(s"Applying normalizer rule 4")
      val c1 = Comp(deepclone(m), q ++ Seq(e1, Gen(idn, e2)) ++ s, e)
      val c2 = Comp(deepclone(m), q ++ Seq(UnaryExp(Not(), deepclone(e1)), Gen(deepclone(idn), e3)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteIdns(c2))
  }

  /** Rewrite all identifiers in the expression using new global identifiers.
    * Takes care to only rewrite identifiers that are system generated, and not user-defined class extent names.
    */
  def rewriteIdns(e: Exp) = {
    var ids = scala.collection.mutable.Map[String, String]()

    def newIdn(idn: Idn) = { if (!ids.contains(idn)) ids.put(idn, SymbolTable.next()); ids(idn) }

    rewrite(
      everywhere(rule[IdnNode] {
        case IdnDef(idn, _) if idn.startsWith("$") => IdnDef(newIdn(idn), None)
        case IdnUse(idn)    if idn.startsWith("$") => IdnUse(newIdn(idn))
      }))(e)
  }

  /** Rule 5
    */

  object Rule5 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ZeroCollectionMonoid) => g})
  }

  def getNumberConst(t: Type, v: Int): NumberConst = t match {
    case _: IntType   => IntConst(v.toString)
    case _: FloatType => FloatConst(v.toString)
    case t1           => throw NormalizerError(s"Unexpected type $t1")
  }

  lazy val rule5 = rule[Exp] {
    case Comp(m, Rule5(q, Gen(idn, ze: ZeroCollectionMonoid), s), e) =>
      logger.debug(s"Applying normalizer rule 5")
      m match {
        case _: MaxMonoid        => getNumberConst(tipe(e), 0)
        case _: MultiplyMonoid   => getNumberConst(tipe(e), 1)
        case _: SumMonoid        => getNumberConst(tipe(e), 0)
        case _: AndMonoid        => BoolConst(true)
        case _: OrMonoid         => BoolConst(false)
        case m: CollectionMonoid => ZeroCollectionMonoid(m)
      }
  }

  /** Rule 6
    */

  object Rule6 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ConsCollectionMonoid) => g})
  }

  lazy val rule6 = rule[Exp] {
    case Comp(m, Rule6(q, Gen(idn, ConsCollectionMonoid(_, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 6")
      Comp(m, q ++ Seq(Bind(idn, e1)) ++ s, e)
  }

  /** Rule 7
    */

  object Rule7 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: MergeMonoid) => g})
  }

  lazy val rule7 = rule[Exp] {
    case Comp(m, Rule7(q, Gen(idn, MergeMonoid(_, e1, e2)), s), e) if m.commutative || q.isEmpty =>
      logger.debug(s"Applying normalizer rule 7")
      val c1 = Comp(deepclone(m), q ++ Seq(Gen(idn, e1)) ++ s, e)
      val c2 = Comp(deepclone(m), q.map(deepclone) ++ Seq(Gen(deepclone(idn), e2)) ++ s.map(deepclone), deepclone(e))
      MergeMonoid(m, c1, rewriteIdns(c2))
  }

  /** Rule 8
    */

  object Rule8 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: Comp) => g})
  }

  lazy val rule8 = rule[Exp] {
    case Comp(m, Rule8(q, Gen(idn, Comp(_, r, e1)), s), e) =>
      logger.debug(s"Applying normalizer rule 8")
      Comp(m, q ++ r ++ Seq(Bind(idn, e1)) ++ s, e)
  }

  /** Rule 9
    */

  object Rule9 {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Comp](qs, { case c @ Comp(_: OrMonoid, _, _) => c})
  }

  lazy val rule9 = rule[Exp] {
    case Comp(m, Rule9(q, Comp(_: OrMonoid, r, pred), s), e) if m.idempotent =>
      logger.debug(s"Applying normalizer rule 9")
      Comp(m, q ++ r ++ Seq(pred) ++ s, e)
  }

  /** Rule 10
    */

  lazy val rule10 = rule[Exp] {
    case Comp(m: PrimitiveMonoid, s, Comp(m1, r, e)) if m == m1 =>
      logger.debug(s"Applying normalizer rule 10")
      Comp(m, s ++ r, e)
  }

}
