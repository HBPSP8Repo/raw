package raw
package calculus

import org.kiama.attribution.Attribution

/** Simplify expressions:
  * - by transforming into the equivalent CNF form;
  * - applying a smaller number of lossless math simplications;
  * - by removing useless conversions to bag/list/set.
  */
class Simplifier(val analyzer: SemanticAnalyzer) extends Attribution with SemanticTransformer {

  import Calculus._
  import org.kiama.rewriting.Rewriter._

  def transform = simplify

  lazy val simplify =
    attempt(reduce(removeUselessTos)) <*
    reduce(
      ruleNotBoolConst +
      ruleTrueOrA + ruleFalseOrA  + ruleTrueAndA + ruleFalseAndA + ruleNotNotA + ruleDeMorgan +
      ruleAorNotA + ruleAandNotA + ruleRepeatedOr + ruleRepeatedAnd + ruleRepeatedAndInOr + ruleRepeatedOrInAnd +
      ruleDistributeAndOverOr + ruleAddZero + ruleSubZero + ruleReplaceSubByNeg + ruleSubSelf +  ruleRemoveDoubleNeg +
      ruleMultiplyByZero + ruleMultiplyByOne + ruleDivideByOne + ruleDivideBySelf + ruleDivDivByMultDiv +
      ruleIfConst)

  /** Remove useless conversions.
    */
  private def isCollectionMonoid(e: Exp, m: CollectionMonoid) =
    analyzer.tipe(e) match {
      case CollectionType(`m`, _) => true
      case _ => false
    }

  private lazy val removeUselessTos = rule[Exp] {
    case u @ UnaryExp(_: ToBag, e) if { logger.debug(s"### $u") ; isCollectionMonoid(e, BagMonoid()) }   => e
    case u @ UnaryExp(_: ToList, e) if { logger.debug(s"### $u") ; isCollectionMonoid(e, ListMonoid()) } => e
    case u @ UnaryExp(_: ToSet, e) if { logger.debug(s"### $u") ; isCollectionMonoid(e, SetMonoid()) }   => e
  }

  /** Rules to simplify Boolean expressions to CNF.
    */

  private lazy val ors: Exp => Set[Exp] = attr {
    case BinaryExp(_: Or, lhs, rhs) => ors(lhs) ++ ors(rhs)
    case e                                  => Set(e)
  }

  private lazy val ands: Exp => Set[Exp] = attr {
    case BinaryExp(_: And, lhs, rhs) => ands(lhs) ++ ands(rhs)
    case e                                   => Set(e)
  }

  /** not(false) => true
    * not(true) => false */
  private lazy val ruleNotBoolConst = rule[Exp] {
    case UnaryExp(_: Not, BoolConst(false)) => BoolConst(true)
    case UnaryExp(_: Not, BoolConst(true)) => BoolConst(false)
  }

  /** true | A => true */
  private lazy val ruleTrueOrA = rule[Exp] {
    case BinaryExp(_: Or, t @ BoolConst(true), _) => logger.debug("ruleTrueOrA"); t
    case BinaryExp(_: Or, _, t @ BoolConst(true)) => logger.debug("ruleTrueOrA"); t
  }

  /** false | A => A */
  private lazy val ruleFalseOrA = rule[Exp] {
    case BinaryExp(_: Or, BoolConst(false), a) => logger.debug("ruleFalseOrA"); a
    case BinaryExp(_: Or, a, BoolConst(false)) => logger.debug("ruleFalseOrA"); a
  }

  /** true & A => A */
  private lazy val ruleTrueAndA = rule[Exp] {
    case BinaryExp(_: And, BoolConst(true), a) => logger.debug("ruleTrueAndA"); a
    case BinaryExp(_: And, a, BoolConst(true)) => logger.debug("ruleTrueAndA"); a
  }

  /** false & A => false */
  private lazy val ruleFalseAndA = rule[Exp] {
    case BinaryExp(_: And, f @ BoolConst(false), _) => logger.debug("ruleFalseAndA"); f
    case BinaryExp(_: And, _, f @ BoolConst(false)) => logger.debug("ruleFalseAndA"); f
  }

  /** !!A => A */
  private lazy val ruleNotNotA = rule[Exp] {
    case UnaryExp(_: Not, UnaryExp(_: Not, a)) => logger.debug("ruleNotNotA"); a
  }

  /** DeMorgan's laws:
    *
    * !(A & B) => !A | !B
    * !(A | B) => !A & !B
    */
  private lazy val ruleDeMorgan = rule[Exp] {
    case UnaryExp(_: Not, BinaryExp(_: And, a, b)) =>
      logger.debug("ruleDeMorgan")
      BinaryExp(Or(), UnaryExp(Not(), a), UnaryExp(Not(), b))
    case UnaryExp(_: Not, BinaryExp(_: Or, a, b))  =>
      logger.debug("ruleDeMorgan")
      BinaryExp(And(), UnaryExp(Not(), a), UnaryExp(Not(), b))
  }

  /** A | !A => true */
  private lazy val ruleAorNotA = rule[Exp] {
    case BinaryExp(_: Or, a, b) if ors(b) contains UnaryExp(Not(), a)  =>
      logger.debug("ruleAorNotA")
      BoolConst(true)
    case BinaryExp(_: Or, UnaryExp(_: Not, a), b) if ors(b) contains a =>
      logger.debug("ruleAorNotA")
      BoolConst(true)
  }

  /** A & !A => false */
  private lazy val ruleAandNotA = rule[Exp] {
    case BinaryExp(_: And, a, b) if ands(b) contains UnaryExp(Not(), a)  =>
      logger.debug("ruleAandNotA")
      BoolConst(false)
    case BinaryExp(_: And, UnaryExp(_: Not, a), b) if ands(b) contains a =>
      logger.debug("ruleAandNotA")
      BoolConst(false)
  }

  /** (A | (A | (B | C))) => (A | (B | C)) */
  private lazy val ruleRepeatedOr = rule[Exp] {
    case BinaryExp(_: Or, a, b) if ors(b) contains a =>
      logger.debug("ruleRepeatedOr")
      b
  }

  /** (A & (A & (B & C))) => (A & (B & C)) */
  private lazy val ruleRepeatedAnd = rule[Exp] {
    case BinaryExp(_: And, a, b) if ands(b) contains a =>
      logger.debug("ruleRepeatedAnd")
      b
  }

  /** (A & B) | (A & B & C)) => (A & B) */
  private lazy val ruleRepeatedAndInOr = rule[Exp] {
    case BinaryExp(_: Or, a, b) if ands(a).nonEmpty && (ands(a) subsetOf ands(b)) =>
      logger.debug("ruleRepeatedAndInOr")
      a
    case BinaryExp(_: Or, a, b) if ands(b).nonEmpty && (ands(b) subsetOf ands(a)) =>
      logger.debug("ruleRepeatedAndInOr")
      b
  }

  /** (A | B) & (A | B | C) => (A | B) */
  private lazy val ruleRepeatedOrInAnd = rule[Exp] {
    case BinaryExp(_: And, a, b) if ors(a).nonEmpty && (ors(a) subsetOf ors(b)) =>
      logger.debug("ruleRepeatedOrInAnd")
      a
    case BinaryExp(_: And, a, b) if ors(b).nonEmpty && (ors(b) subsetOf ors(a)) =>
      logger.debug("ruleRepeatedOrInAnd")
      b
  }

  /** (P1 & P2 & P3) | (Q1 & Q2 & Q3) =>
    * (P1 | Q1) & (P1 | Q2) & (P1 | Q3) &
    * (P2 | Q1) & (P2 | Q2) & (P2 | Q3) &
    * (P3 | Q1) & (P3 | Q2) & (P3 | Q3) &
    */
  private lazy val ruleDistributeAndOverOr = rule[Exp] {
    case BinaryExp(_: Or, a, b) if ands(a).size > 2 && ands(b).size > 2 =>
      logger.debug("ruleDistributeAndOverOr")
      val prod = for (x <- ands(a); y <- ands(b)) yield BinaryExp(Or(), x, y)
      val head = prod.head
      val rest = prod.drop(1)
      rest.foldLeft(head)((a, b) => BinaryExp(And(), a, b))
  }

  /** The symmetric rule to the above, i.e.:
    *   (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
    * is handled indirectly by `ands`.
    */

  private object ExtractNumberConst {
    def unapply(c: NumberConst): Option[String] = c match {
      case IntConst(v)   => Some(v)
      case FloatConst(v) => Some(v)
      case _             => None
    }
  }

  /** Rules to simplify algebraic expressions.
    */

  /** x + 0 => x */
  private lazy val ruleAddZero = rule[Exp] {
    case BinaryExp(_: Plus, lhs, ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleAddZero")
      lhs
    case BinaryExp(_: Plus, ExtractNumberConst(v), rhs) if v.toFloat == 0 =>
      logger.debug("ruleAddZero")
      rhs
  }

  /** x - 0 => x */
  private lazy val ruleSubZero = rule[Exp] {
    case BinaryExp(_: Sub, lhs, ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleSubZero")
      lhs
  }

  /** a - b => a + (-b) */
  private lazy val ruleReplaceSubByNeg = rule[Exp] {
    case BinaryExp(_: Sub, lhs, rhs) =>
      logger.debug("ruleReplaceSubByNeg")
      BinaryExp(Plus(), lhs, UnaryExp(Neg(), rhs))
  }

  /** x + (-x) => 0 */
  private lazy val ruleSubSelf = rule[Exp] {
    case BinaryExp(_: Plus, lhs, UnaryExp(_: Neg, rhs)) if lhs == rhs =>
      logger.debug("ruleSubSelf")
      IntConst("0")
  }

  /** --x => x */
  private lazy val ruleRemoveDoubleNeg = rule[Exp] {
    case UnaryExp(_: Neg, UnaryExp(_: Neg, e)) =>
      logger.debug("ruleRemoveDoubleNeg")
      e
  }

  /** x * 0 => 0 */
  private lazy val ruleMultiplyByZero = rule[Exp] {
    case BinaryExp(_: Mult, lhs, c @ ExtractNumberConst(v)) if v.toFloat == 0 =>
      logger.debug("ruleMultiplyByZero")
      c
    case BinaryExp(_: Mult, c @ ExtractNumberConst(v), rhs) if v.toFloat == 0 =>
      logger.debug("ruleMultiplyByZero")
      c
  }

  /** x * 1 => x */
  private lazy val ruleMultiplyByOne = rule[Exp] {
    case BinaryExp(_: Mult, lhs, ExtractNumberConst(v)) if v.toFloat == 1 =>
      logger.debug("ruleMultiplyByOne")
      lhs
    case BinaryExp(_: Mult, ExtractNumberConst(v), rhs) if v.toFloat == 1 =>
      logger.debug("ruleMultiplyByOne")
      rhs
  }

  /** x / 1 => x */
  private lazy val ruleDivideByOne = rule[Exp] {
    case BinaryExp(_: Div, lhs, ExtractNumberConst(v)) if v.toFloat == 1 =>
      logger.debug("ruleDivideByOne")
      lhs
  }

  /** x / x => 1 */
  private lazy val ruleDivideBySelf = rule[Exp] {
    case BinaryExp(_: Div, lhs, rhs) if lhs == rhs =>
      logger.debug("ruleDivideBySelf")
      IntConst("1")
  }

  /** x / (y / z) => x * z / y */
  private lazy val ruleDivDivByMultDiv = rule[Exp] {
    case BinaryExp(_: Div, x, BinaryExp(_: Div, y, z)) =>
      logger.debug("ruleDivDivByMultDiv")
      BinaryExp(Div(), BinaryExp(Mult(), x, z), y)
  }

  /** if (true)  then e1 else e2 => e1
   *  if (false) then e1 else e2 => e2
   */
  private lazy val ruleIfConst = rule[Exp] {
    case IfThenElse(BoolConst(true), e1, _) => e1
    case IfThenElse(BoolConst(false), _, e2) => e2
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
