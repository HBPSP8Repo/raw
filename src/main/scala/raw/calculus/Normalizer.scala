package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

case class NormalizerError(err: String) extends RawException(err)

/** Normalize a comprehension.
  */
import scala.collection.immutable.Seq

object Normalizer extends LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import org.kiama.rewriting.Strategy
  import Calculus._

  /** Transform a tree into its normalized form.
    * The variable names must be globally unique.
    * The normalization rules are described in [1] (Fig. 4, page 17).
    */
  def apply(tree: Calculus, world: World): Calculus = {
    val inTree = Uniquifier(tree, world)
    logger.debug(s"Normalizer input tree: ${CalculusPrettyPrinter.pretty(tree.root)}")

    val analyzer = new SemanticAnalyzer(tree, world)

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

    /** Rule 1
      */
    object Rule1 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b: Bind => b})
    }

    lazy val rule1 = rule[Comp] {
      case Comp(m, Rule1(r, Bind(IdnDef(x), u), s), e) =>
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
      case f @ FunApp(FunAbs(IdnDef(idn), _, e1), e2) => {
        logger.debug(s"Applying normalizer rule 2")
        rewrite(everywhere(rule[Exp] {
          case IdnExp(IdnUse(`idn`)) => deepclone(e2)
        }))(e1)
      }
    }

    /** Rule 3
      */
    lazy val rule3 = rule[Exp] {
      case r @ RecordProj(RecordCons(atts), idn) => {
        logger.debug(s"Applying normalizer rule 3")
        atts.collect { case att if att.idn == idn => att.e}.head
      }
    }

    /** Rule 4
      */

    object Rule4 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: IfThenElse) => g})
    }

    lazy val rule4 = rule[Exp] {
      case Comp(m, Rule4(q, Gen(idn, IfThenElse(e1, e2, e3)), s), e) if m.commutative || q.isEmpty => {
        logger.debug(s"Applying normalizer rule 4")
        val c1 = Comp(deepclone(m), q ++ Seq(e1, Gen(idn, e2)) ++ s, e)
        val c2 = Comp(deepclone(m), q ++ Seq(UnaryExp(Not(), deepclone(e1)), Gen(deepclone(idn), e3)) ++ s.map(deepclone(_)), deepclone(e))
        MergeMonoid(m, c1, rewriteIdns(c2))
      }
    }

    /** Rewrite all identifiers in the expression using new global identifiers.
      * Takes care to only rewrite identifiers that are system generated, and not user-defined class extent names.
      */
    def rewriteIdns(e: Exp) = {
      var ids = scala.collection.mutable.Map[String, String]()

      def newIdn(idn: Idn) = { if (!ids.contains(idn)) ids.put(idn, SymbolTable.next()); ids(idn) }

      rewrite(
        everywhere(rule[IdnNode] {
          case IdnDef(idn) if idn.startsWith("$") => IdnDef(newIdn(idn))
          case IdnUse(idn) if idn.startsWith("$") => IdnUse(newIdn(idn))
        }))(e)
    }

    /** Rule 5
      */

    object Rule5 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ZeroCollectionMonoid) => g})
    }

    // TODO: Method repeated on the Simplifier!!!
    def getNumberConst(t: Type, v: Int): NumberConst = t match {
      case _: IntType   => IntConst(v.toString)
      case _: FloatType => FloatConst(v.toString)
      case t            => throw NormalizerError(s"Unexpected type $t")
    }

    lazy val rule5 = rule[Exp] {
      case Comp(m, Rule5(q, Gen(idn, ze: ZeroCollectionMonoid), s), e) => {
        logger.debug(s"Applying normalizer rule 5")
        m match {
          // TODO: Add proper zero monoids for primitive types: e.g. -infinity
          case _: MaxMonoid        => getNumberConst(analyzer.tipe(e), 0)
          case _: MultiplyMonoid   => getNumberConst(analyzer.tipe(e), 1)
          case _: SumMonoid        => getNumberConst(analyzer.tipe(e), 0)
          case _: AndMonoid        => BoolConst(true)
          case _: OrMonoid         => BoolConst(false)
          case m: CollectionMonoid => ZeroCollectionMonoid(m)
        }
      }
    }

    /** Rule 6
      */

    object Rule6 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: ConsCollectionMonoid) => g})
    }

    lazy val rule6 = rule[Exp] {
      case Comp(m, Rule6(q, Gen(idn, ConsCollectionMonoid(_, e1)), s), e) => {
        logger.debug(s"Applying normalizer rule 6")
        Comp(m, q ++ Seq(Bind(idn, e1)) ++ s, e)
      }
    }

    /** Rule 7
      */

    object Rule7 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: MergeMonoid) => g})
    }

    lazy val rule7 = rule[Exp] {
      case Comp(m, Rule7(q, Gen(idn, MergeMonoid(_, e1, e2)), s), e) if m.commutative || q.isEmpty => {
        logger.debug(s"Applying normalizer rule 7")
        val c1 = Comp(deepclone(m), q ++ Seq(Gen(idn, e1)) ++ s, e)
        val c2 = Comp(deepclone(m), q.map(deepclone(_)) ++ Seq(Gen(deepclone(idn), e2)) ++ s.map(deepclone(_)), deepclone(e))
        MergeMonoid(m, c1, rewriteIdns(c2))
      }
    }

    /** Rule 8
      */

    object Rule8 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_, _: Comp) => g})
    }

    lazy val rule8 = rule[Exp] {
      case Comp(m, Rule8(q, Gen(idn, Comp(_, r, e1)), s), e) => {
        logger.debug(s"Applying normalizer rule 8")
        Comp(m, q ++ r ++ Seq(Bind(idn, e1)) ++ s, e)
      }
    }

    /** Rule 9
      */

    object Rule9 {
      def unapply(qs: Seq[Qual]) = splitWith[Qual, Comp](qs, { case c @ Comp(_: OrMonoid, _, _) => c})
    }

    lazy val rule9 = rule[Exp] {
      case Comp(m, Rule9(q, Comp(_: OrMonoid, r, pred), s), e) if m.idempotent => {
        logger.debug(s"Applying normalizer rule 9")
        Comp(m, q ++ r ++ Seq(pred) ++ s, e)
      }
    }

    /** Rule 10
      */
    lazy val rule10 = rule[Exp] {
      case Comp(m: PrimitiveMonoid, s, Comp(m1, r, e)) if m == m1 => {
        logger.debug(s"Applying normalizer rule 10")
        Comp(m, s ++ r, e)
      }
    }

    lazy val strategy: Strategy = doloop(reduce(rule1), oncetd(rule2 + rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10)) //attempt(everywhere(rule1)) <* (oncetd(rule2 + rule3 + rule4 + rule5 + rule6 + rule7 + rule8 + rule9 + rule10) < strategy + id)

    val outTree = rewriteTree(strategy)(inTree)
    logger.debug(s"Normalizer output tree: ${CalculusPrettyPrinter.pretty(outTree.root)}")
    outTree
  }
}
