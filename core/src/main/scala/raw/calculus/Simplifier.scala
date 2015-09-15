package raw
package calculus

/** Simplify expressions by transforming into the equivalent CNF form with a smaller number of lossless math simplications.
  */
trait Simplifier extends Normalizer {

  import org.kiama.rewriting.Rewriter._
  import Calculus._

  override def strategy = attempt(super.strategy) <* simplify

  // TODO: Compute expressions like "if (true) then 1 else 2"
  // TODO: Fold constants

  lazy val simplify = reduce(ruleTrueOrA + ruleFalseOrA  + ruleTrueAndA + ruleFalseAndA + ruleNotNotA + ruleDeMorgan +
    ruleAorNotA + ruleAandNotA + ruleRepeatedOr + ruleRepeatedAnd + ruleRepeatedAndInOr + ruleRepeatedOrInAnd +
    ruleDistributeAndOverOr + ruleAddZero + ruleSubZero + ruleReplaceSubByNeg + ruleSubSelf +  ruleRemoveDoubleNeg +
    ruleMultiplyByZero + ruleMultiplyByOne + ruleDivideByOne + ruleDivideBySelf + ruleDivDivByMultDiv)
  //ruleDivideConstByConst + ruleDropNeg + ruleDropConstCast + ruleDropConstComparison + ruleFoldConsts)

  /** Rules to simplify Boolean expressions to CNF.
    */

  def ors(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: OrMonoid, lhs, rhs) => ors(lhs) ++ ors(rhs)
    case _                                  => Set(e)
  }

  def ands(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: AndMonoid, lhs, rhs) => ands(lhs) ++ ands(rhs)
    case _                                   => Set(e)
  }

  /** true | A => true */
  lazy val ruleTrueOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, t @ BoolConst(true), _) => logger.debug("ruleTrueOrA"); t
    case MergeMonoid(_: OrMonoid, _, t @ BoolConst(true)) => logger.debug("ruleTrueOrA"); t
  }

  /** false | A => A */
  lazy val ruleFalseOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, BoolConst(false), a) => logger.debug("ruleFalseOrA"); a
    case MergeMonoid(_: OrMonoid, a, BoolConst(false)) => logger.debug("ruleFalseOrA"); a
  }

  /** true & A => A */
  lazy val ruleTrueAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, BoolConst(true), a) => logger.debug("ruleTrueAndA"); a
    case MergeMonoid(_: AndMonoid, a, BoolConst(true)) => logger.debug("ruleTrueAndA"); a
  }

  /** false & A => false */
  lazy val ruleFalseAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, f @ BoolConst(false), _) => logger.debug("ruleFalseAndA"); f
    case MergeMonoid(_: AndMonoid, _, f @ BoolConst(false)) => logger.debug("ruleFalseAndA"); f
  }

  /** !!A => A */
  lazy val ruleNotNotA = rule[Exp] {
    case UnaryExp(_: Not, UnaryExp(_: Not, a)) => logger.debug("ruleNotNotA"); a
  }

  /** DeMorgan's laws:
    *
    * !(A & B) => !A | !B
    * !(A | B) => !A & !B
    */
  lazy val ruleDeMorgan = rule[Exp] {
    case UnaryExp(_: Not, MergeMonoid(_: AndMonoid, a, b)) =>
      logger.debug("ruleDeMorgan")
      MergeMonoid(OrMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
    case UnaryExp(_: Not, MergeMonoid(_: OrMonoid, a, b))  =>
      logger.debug("ruleDeMorgan")
      MergeMonoid(AndMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
  }

  /** A | !A => true */
  lazy val ruleAorNotA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains UnaryExp(Not(), a)  =>
      logger.debug("ruleAorNotA")
      BoolConst(true)
    case MergeMonoid(_: OrMonoid, UnaryExp(_: Not, a), b) if ors(b) contains a =>
      logger.debug("ruleAorNotA")
      BoolConst(true)
  }

  /** A & !A => false */
  lazy val ruleAandNotA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains UnaryExp(Not(), a)  =>
      logger.debug("ruleAandNotA")
      BoolConst(false)
    case MergeMonoid(_: AndMonoid, UnaryExp(_: Not, a), b) if ands(b) contains a =>
      logger.debug("ruleAandNotA")
      BoolConst(false)
  }

  /** (A | (A | (B | C))) => (A | (B | C)) */
  lazy val ruleRepeatedOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains a =>
      logger.debug("ruleRepeatedOr")
      b
  }

  /** (A & (A & (B & C))) => (A & (B & C)) */
  lazy val ruleRepeatedAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains a =>
      logger.debug("ruleRepeatedAnd")
      b
  }

  /** (A & B) | (A & B & C)) => (A & B) */
  lazy val ruleRepeatedAndInOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ands(a).nonEmpty && (ands(a) subsetOf ands(b)) =>
      logger.debug("ruleRepeatedAndInOr")
      a
    case MergeMonoid(_: OrMonoid, a, b) if ands(b).nonEmpty && (ands(b) subsetOf ands(a)) =>
      logger.debug("ruleRepeatedAndInOr")
      b
  }

  /** (A | B) & (A | B | C) => (A | B) */
  lazy val ruleRepeatedOrInAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ors(a).nonEmpty && (ors(a) subsetOf ors(b)) =>
      logger.debug("ruleRepeatedOrInAnd")
      a
    case MergeMonoid(_: AndMonoid, a, b) if ors(b).nonEmpty && (ors(b) subsetOf ors(a)) =>
      logger.debug("ruleRepeatedOrInAnd")
      b
  }

  /* (P1 & P2 & P3) | (Q1 & Q2 & Q3) =>
   *   (P1 | Q1) & (P1 | Q2) & (P1 | Q3) &
   *   (P2 | Q1) & (P2 | Q2) & (P2 | Q3) &
   *   (P3 | Q1) & (P3 | Q2) & (P3 | Q3) &
   */
  lazy val ruleDistributeAndOverOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ands(a).size > 2 && ands(b).size > 2 =>
      logger.debug("ruleDistributeAndOverOr")
      val prod = for (x <- ands(a); y <- ands(b)) yield MergeMonoid(OrMonoid(), x, y)
      val head = prod.head
      val rest = prod.drop(1)
      rest.foldLeft(head)((a, b) => MergeMonoid(AndMonoid(), a, b))
  }

  /** The symmetric rule to the above, i.e.:
    *   (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
    * is handled indirectly by `ands`.
    */

  object ExtractNumberConst {
    def unapply(c: NumberConst): Option[String] = c match {
      case IntConst(v)   => Some(v)
      case FloatConst(v) => Some(v)
      case _             => None
    }
  }

  /** Rules to simplify algebraic expressions.
    */

  /** x + 0 => x */
  lazy val ruleAddZero = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleAddZero")
      lhs
    case MergeMonoid(_: SumMonoid, ExtractNumberConst(v), rhs) if v.toFloat == 0 =>
      logger.debug("ruleAddZero")
      rhs
  }

  /** x - 0 => x */
  lazy val ruleSubZero = rule[Exp] {
    case BinaryExp(_: Sub, lhs, ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleSubZero")
      lhs
  }

  /** a - b => a + (-b) */
  lazy val ruleReplaceSubByNeg = rule[Exp] {
    case BinaryExp(_: Sub, lhs, rhs) =>
      logger.debug("ruleReplaceSubByNeg")
      MergeMonoid(SumMonoid(), lhs, UnaryExp(Neg(), rhs))
  }

  /** x + (-x) => 0 */
  lazy val ruleSubSelf = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, UnaryExp(_: Neg, rhs)) if lhs == rhs =>
      logger.debug("ruleSubSelf")
      IntConst("0")
  }

  /** --x => x */
  lazy val ruleRemoveDoubleNeg = rule[Exp] {
    case UnaryExp(_: Neg, UnaryExp(_: Neg, e)) =>
      logger.debug("ruleRemoveDoubleNeg")
      e
  }

  /** x * 0 => 0 */
  lazy val ruleMultiplyByZero = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, c @ ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleMultiplyByZero")
      c
    case MergeMonoid(_: MultiplyMonoid, c @ ExtractNumberConst(v), rhs) if v.toFloat == 0 =>
      logger.debug("ruleMultiplyByZero")
      c
  }

  /** x * 1 => x */
  lazy val ruleMultiplyByOne = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, ExtractNumberConst(v)) if v.toFloat == 1 =>
      logger.debug("ruleMultiplyByOne")
      lhs
    case MergeMonoid(_: MultiplyMonoid, ExtractNumberConst(v), rhs) if v.toFloat == 1 =>
      logger.debug("ruleMultiplyByOne")
      rhs
  }

  /** x / 1 => x */
  lazy val ruleDivideByOne = rule[Exp] {
    case BinaryExp(_: Div, lhs, ExtractNumberConst(v)) if v.toFloat == 1 =>
      logger.debug("ruleDivideByOne")
      lhs
  }

  /** x / x => 1 */
  lazy val ruleDivideBySelf = rule[Exp] {
    case BinaryExp(_: Div, lhs, rhs) if lhs == rhs =>
      logger.debug("ruleDivideBySelf")
      IntConst("1")
  }

  /** x / (y / z) => x * z / y */
  lazy val ruleDivDivByMultDiv = rule[Exp] {
    case BinaryExp(_: Div, x, BinaryExp(_: Div, y, z)) =>
      logger.debug("ruleDivDivByMultDiv")
      BinaryExp(Div(), MergeMonoid(MultiplyMonoid(), x, z), y)
  }

  //
  //  /** 1 / 2 => 0
  //    * 1 / 2.0 => 0.5
  //    * 1.0 / 2 => 0.5
  //    * 1.0 / 2.0 => 0.5
  //    */
  //  lazy val ruleDivideConstByConst = rule[Exp] {
  //    case BinaryExp(_: Div, IntConst(lhs), IntConst(rhs))     => IntConst((lhs / rhs).toInt)
  //    case BinaryExp(_: Div, IntConst(lhs), FloatConst(rhs))   => FloatConst(lhs / rhs)
  //    case BinaryExp(_: Div, FloatConst(lhs), IntConst(rhs))   => FloatConst(lhs / rhs)
  //    case BinaryExp(_: Div, FloatConst(lhs), FloatConst(rhs)) => FloatConst(lhs / rhs)
  //  }
  //
  //  /** Neg(1) => -1 (i.e. remove Neg() from constants) */
  //  lazy val ruleDropNeg = rule[Exp] {
  //    // This is invalid. Must fix the precision.
  //    // Pass all int / float consts to Strings.
  //    case UnaryExp(_: Neg, IntConst(v))   => IntConst(-v)
  //    case UnaryExp(_: Neg, FloatConst(v)) => FloatConst(-v)
  //  }
  //
  //  /** Remove constants for casts */
  //  lazy val ruleDropConstCast = rule[Exp] {
  //    case UnaryExp(_: ToBool, IntConst(v))     => BoolConst(if (v == 1) true else false)
  //    case UnaryExp(_: ToBool, FloatConst(v))   => BoolConst(if (v == 1) true else false)
  //    case UnaryExp(_: ToBool, StringConst(v))  => BoolConst(if (v.toLowerCase == "true") true else false)
  //    case UnaryExp(_: ToFloat, BoolConst(v))   => FloatConst(if (v) 1 else 0)
  //    case UnaryExp(_: ToFloat, IntConst(v))    => FloatConst(v.toFloat)
  //    case UnaryExp(_: ToFloat, StringConst(v)) => FloatConst(v.toFloat)
  //    case UnaryExp(_: ToInt, BoolConst(v))     => IntConst(if (v) 1 else 0)
  //    case UnaryExp(_: ToInt, FloatConst(v))    => IntConst(v.toInt)
  //    case UnaryExp(_: ToInt, StringConst(v))   => IntConst(v.toInt)
  //    case UnaryExp(_: ToString, BoolConst(v))  => StringConst(v.toString())
  //    case UnaryExp(_: ToString, FloatConst(v)) => StringConst(v.toString())
  //    case UnaryExp(_: ToString, IntConst(v))   => StringConst(v.toString())
  //  }
  //
  //  /** Remove comparisons of constants.
  //    * 1 > 2
  //    * ...
  //    * */
  //  lazy val ruleDropConstComparison = rule[Exp] {
  //    case BinaryExp(_: Eq, lhs: Const, rhs: Const)           => BoolConst(lhs == rhs)
  //    case BinaryExp(_: Neq, lhs: Const, rhs: Const)          => BoolConst(lhs != rhs)
  //    case BinaryExp(_: Ge, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value >= rhs.value)
  //    case BinaryExp(_: Ge, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value >= rhs.value)
  //    case BinaryExp(_: Gt, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value > rhs.value)
  //    case BinaryExp(_: Gt, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value > rhs.value)
  //    case BinaryExp(_: Le, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value <= rhs.value)
  //    case BinaryExp(_: Le, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value <= rhs.value)
  //    case BinaryExp(_: Lt, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value < rhs.value)
  //    case BinaryExp(_: Lt, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value < rhs.value)
  //  }
  //
  //  /** Rules for folding constants across a sequence of additions, multiplications or max.
  //    * e.g: 1 + (x + 1) => 2 + x
  //    */
  //
  //  def merges(m: NumberMonoid, e: Exp): List[Exp] = e match {
  //    case MergeMonoid(`m`, lhs, rhs) => merges(m, lhs) ++ merges(m, rhs)
  //    case e                          => List(e)
  //  }
  //
  //  def hasNumber(m: NumberMonoid, e: Exp) = merges(m, e).collectFirst{ case _: NumberConst => true }.isDefined
  //
  //  def foldConsts(m: NumberMonoid, e1: NumberConst, e2: Exp) = {
  //
  //    /** This "monster" splits the output of merges(m, e2) into two lists in a typesafe way,
  //      * one containing the constants, the other containing the rest.
  //      * It does it in a single pass, accumulating results in a tuple with both lists.
  //      */
  //    val (consts, rest) = merges(m, e2).foldLeft(List[NumberConst](), List[Exp]()) {
  //      case ((cs, es), c: NumberConst) => (cs :+ c, es)
  //      case ((cs, es), e: Exp)         => (cs, es :+ e)
  //    }
  //
  //    val const = e1 match {
  //      case IntConst(v) =>
  //        IntConst(consts.map(_.value.asInstanceOf[Int]).foldLeft(v)((a,b) => m match {
  //          case _: SumMonoid => a + b
  //          case _: MaxMonoid => math.max(a, b)
  //          case _: MultiplyMonoid => a * b
  //        }))
  //      case FloatConst(v) =>
  //        FloatConst(consts.map(_.value.asInstanceOf[Float]).foldLeft(v)((a,b) => m match {
  //          case _: SumMonoid => a + b
  //          case _: MaxMonoid => math.max(a, b)
  //          case _: MultiplyMonoid => a * b
  //        }))
  //    }
  //    if (rest.isEmpty) {
  //      const
  //    } else {
  //      val nrhs = rest.tail.foldLeft(rest.head)((a: Exp, b: Exp) => MergeMonoid(m, a, b))
  //      MergeMonoid(m, const, nrhs)
  //    }
  //  }
  //
  //  // `hasNumber` followed by `splitOnNumbers` is inefficient
  //  lazy val ruleFoldConsts = rule[Exp] {
  //   case MergeMonoid(m: NumberMonoid, lhs: NumberConst, rhs) if hasNumber(m, rhs) => {
  //     logger.debug("ruleFoldConsts")
  //     foldConsts(m, lhs, rhs)
  //   }
  //   case MergeMonoid(m: NumberMonoid, lhs, rhs: NumberConst) if hasNumber(m, lhs) => {
  //     logger.debug("ruleFoldConsts")
  //     foldConsts(m, rhs, lhs)
  //   }
  //  }

}


object Simplifier {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree)
    val a = new SemanticAnalyzer(t1, world)
    val simplifier = new Simplifier {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(simplifier.strategy)(t1)
  }
}
