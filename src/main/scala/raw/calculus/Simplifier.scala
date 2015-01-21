package raw.calculus

import com.typesafe.scalalogging.LazyLogging
import raw._

trait Simplifier extends Canonizer with LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import CanonicalCalculus._

  def simplify(c: Calculus.Comp): Comp = {
    val c1 = canonize(c)
    simplificationRules(c1) match {
      case Some(c2: Comp) => c2
      case _              => c1
    }
  }

  lazy val simplificationRules = reduce(
    ruleTrueOrA +
    ruleFalseOrA +
    ruleTrueAndA +
    ruleFalseAndA +
    ruleNotNotA +
    ruleDeMorgan +
    ruleAorNotA +
    ruleAandNotA +
    ruleRepeatedOr +
    ruleRepeatedAnd +
    ruleRepeatedAndInOr +
    ruleRepeateOrInAnd +
    ruleDistributeAndOverOr +
    ruleAddZero +
    ruleSubZero +
    ruleReplaceSubByNeg +
    ruleSubSelf +
    ruleRemoveDoubleNeg +
    ruleMultiplyByZero +
    ruleMultiplyByOne +
    ruleDivideByOne +
    ruleDivideBySelf +
    ruleDivDivByMultDiv +
    ruleDivideConstByConst +
    ruleDropNeg +
    ruleDropConstCast +
    ruleDropConstComparison +
    ruleFoldConsts)

  def ors(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: OrMonoid, lhs, rhs) => ors(lhs) ++ ors(rhs)
    case e                                  => Set(e)
  }

  def ands(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: AndMonoid, lhs, rhs) => ands(lhs) ++ ands(rhs)
    case e                                   => Set(e)
  }

  /** Rules to simplify Boolean expressions to CNF.
    */

  /** true | A => true */
  lazy val ruleTrueOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, t @ BoolConst(true), _) => t
    case MergeMonoid(_: OrMonoid, _, t @ BoolConst(true)) => t
  }

  /** false | A => A */
  lazy val ruleFalseOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, BoolConst(false), a) => a
    case MergeMonoid(_: OrMonoid, a, BoolConst(false)) => a
  }

  /** true & A => A */
  lazy val ruleTrueAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, BoolConst(true), a) => a
    case MergeMonoid(_: AndMonoid, a, BoolConst(true)) => a
  }

  /** false & A => false */
  lazy val ruleFalseAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, f @ BoolConst(false), _) => f
    case MergeMonoid(_: AndMonoid, _, f @ BoolConst(false)) => f
  }

  /** !!A => A */
  lazy val ruleNotNotA = rule[Exp] {
    case UnaryExp(_: Not, UnaryExp(_: Not, a)) => a
  }

  /** DeMorgan's laws:
    *
    * !(A & B) => !A | !B
    * !(A | B) => !A & !B
    */
  lazy val ruleDeMorgan = rule[Exp] {
    case UnaryExp(_: Not, MergeMonoid(_: AndMonoid, a, b)) => MergeMonoid(OrMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
    case UnaryExp(_: Not, MergeMonoid(_: OrMonoid, a, b))  => MergeMonoid(AndMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
  }

  /** A | !A => true */
  lazy val ruleAorNotA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains UnaryExp(Not(), a)  => BoolConst(true)
    case MergeMonoid(_: OrMonoid, UnaryExp(_: Not, a), b) if ors(b) contains a => BoolConst(true)
  }

  /** A & !A => false */
  lazy val ruleAandNotA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains UnaryExp(Not(), a)  => BoolConst(false)
    case MergeMonoid(_: AndMonoid, UnaryExp(_: Not, a), b) if ands(b) contains a => BoolConst(false)
  }

  /** (A | (A | (B | C))) => (A | (B | C)) */
  lazy val ruleRepeatedOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains a => b
  }

  /** (A & (A & (B & C))) => (A & (B & C)) */
  lazy val ruleRepeatedAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains a => b
  }

  /** (A & B) | (A & B & C)) => (A & B) */
  lazy val ruleRepeatedAndInOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if !ands(a).isEmpty && (ands(a) subsetOf ands(b)) => a
    case MergeMonoid(_: OrMonoid, a, b) if !ands(b).isEmpty && (ands(b) subsetOf ands(a)) => b
  }

  /** (A | B) & (A | B | C) => (A | B) */
  lazy val ruleRepeateOrInAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if !ors(a).isEmpty && (ors(a) subsetOf ors(b)) => a
    case MergeMonoid(_: AndMonoid, a, b) if !ors(b).isEmpty && (ors(b) subsetOf ors(a)) => b
  }

  /* (P1 & P2 & P3) | (Q1 & Q2 & Q3) =>
   *   (P1 | Q1) & (P1 | Q2) & (P1 | Q3) &
   *   (P2 | Q1) & (P2 | Q2) & (P2 | Q3) &
   *   (P3 | Q1) & (P3 | Q2) & (P3 | Q3) &
   */
  lazy val ruleDistributeAndOverOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if !ands(a).isEmpty && !ands(b).isEmpty => {
      val prod = for (x <- ands(a); y <- ands(b)) yield MergeMonoid(OrMonoid(), x, y)
      val head = prod.head
      val rest = prod.drop(1)
      rest.foldLeft(head)((a, b) => MergeMonoid(AndMonoid(), a, b))
    }
  }

  /** The symmetric rule to the above, i.e.:
    *   (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
    * is handled indirectly by `ands`.
     */

  /** Rules to simplify algebraic expressions.
    */

  /** x + 0 => x */
  lazy val ruleAddZero = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, IntConst(v))   if v == 0 => lhs
    case MergeMonoid(_: SumMonoid, IntConst(v), rhs)   if v == 0 => rhs
    case MergeMonoid(_: SumMonoid, lhs, FloatConst(v)) if v == 0 => lhs
    case MergeMonoid(_: SumMonoid, FloatConst(v), rhs) if v == 0 => rhs
  }

  /** x - 0 => x */
  lazy val ruleSubZero = rule[Exp] {
    case BinaryExp(_: Sub, lhs, IntConst(v))   if v == 0 => lhs
    case BinaryExp(_: Sub, lhs, FloatConst(v)) if v == 0 => lhs
  }

  /** a - b => a + (-b) */
  lazy val ruleReplaceSubByNeg = rule[Exp] {
    case BinaryExp(_: Sub, lhs, rhs) => MergeMonoid(SumMonoid(), lhs, UnaryExp(Neg(), rhs))
  }

  /** x + (-x) => 0 */
  lazy val ruleSubSelf = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, UnaryExp(_: Neg, rhs)) if lhs == rhs => IntConst(0)
  }

  /** --x => x */
  lazy val ruleRemoveDoubleNeg = rule[Exp] {
    case UnaryExp(_: Neg, UnaryExp(_: Neg, e)) => e
  }

  /** x * 0 => 0 */
  lazy val ruleMultiplyByZero = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, c @ IntConst(v))   if v == 0 => c
    case MergeMonoid(_: MultiplyMonoid, c @ IntConst(v), rhs)   if v == 0 => c
    case MergeMonoid(_: MultiplyMonoid, lhs, c @ FloatConst(v)) if v == 0 => c
    case MergeMonoid(_: MultiplyMonoid, c @ FloatConst(v), rhs) if v == 0 => c
  }

  /** x * 1 => x */
  lazy val ruleMultiplyByOne = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, c @ IntConst(v))   if v == 1 => lhs
    case MergeMonoid(_: MultiplyMonoid, c @ IntConst(v), rhs)   if v == 1 => rhs
    case MergeMonoid(_: MultiplyMonoid, lhs, c @ FloatConst(v)) if v == 1 => lhs
    case MergeMonoid(_: MultiplyMonoid, c @ FloatConst(v), rhs) if v == 1 => rhs
  }

  /** x / 1 => x */
  lazy val ruleDivideByOne = rule[Exp] {
    case BinaryExp(_: Div, lhs, c @ IntConst(v)) if v == 1   => lhs
    case BinaryExp(_: Div, lhs, c @ FloatConst(v)) if v == 1 => lhs
  }

  /** x / x => 1 */
  lazy val ruleDivideBySelf = rule[Exp] {
    case BinaryExp(_: Div, lhs, rhs) if lhs == rhs => IntConst(1)
  }

  /** x / (y / z) => x * z / y */
  lazy val ruleDivDivByMultDiv = rule[Exp] {
    case BinaryExp(_: Div, x, BinaryExp(_: Div, y, z)) => BinaryExp(Div(), MergeMonoid(MultiplyMonoid(), x, z), y)
  }

  /** 1 / 2 => 0
    * 1 / 2.0 => 0.5
    * 1.0 / 2 => 0.5
    * 1.0 / 2.0 => 0.5
    */
  lazy val ruleDivideConstByConst = rule[Exp] {
    case BinaryExp(_: Div, IntConst(lhs), IntConst(rhs))     => IntConst((lhs / rhs).toInt)
    case BinaryExp(_: Div, IntConst(lhs), FloatConst(rhs))   => FloatConst(lhs / rhs)
    case BinaryExp(_: Div, FloatConst(lhs), IntConst(rhs))   => FloatConst(lhs / rhs)
    case BinaryExp(_: Div, FloatConst(lhs), FloatConst(rhs)) => FloatConst(lhs / rhs)
  }

  /** Neg(1) => -1 (i.e. remove Neg() from constants) */
  lazy val ruleDropNeg = rule[Exp] {
    case UnaryExp(_: Neg, IntConst(v))   => IntConst(-v)
    case UnaryExp(_: Neg, FloatConst(v)) => FloatConst(-v)
  }

  /** Remove constants for casts */
  lazy val ruleDropConstCast = rule[Exp] {
    case UnaryExp(_: ToBool, IntConst(v))     => BoolConst(if (v == 1) true else false)
    case UnaryExp(_: ToBool, FloatConst(v))   => BoolConst(if (v == 1) true else false)
    case UnaryExp(_: ToBool, StringConst(v))  => BoolConst(if (v.toLowerCase == "true") true else false)
    case UnaryExp(_: ToFloat, BoolConst(v))   => FloatConst(if (v) 1 else 0)
    case UnaryExp(_: ToFloat, IntConst(v))    => FloatConst(v.toFloat)
    case UnaryExp(_: ToFloat, StringConst(v)) => FloatConst(v.toFloat)
    case UnaryExp(_: ToInt, BoolConst(v))     => IntConst(if (v) 1 else 0)
    case UnaryExp(_: ToInt, FloatConst(v))    => IntConst(v.toInt)
    case UnaryExp(_: ToInt, StringConst(v))   => IntConst(v.toInt)
    case UnaryExp(_: ToString, BoolConst(v))  => StringConst(v.toString())
    case UnaryExp(_: ToString, FloatConst(v)) => StringConst(v.toString())
    case UnaryExp(_: ToString, IntConst(v))   => StringConst(v.toString())
  }

  /** Remove comparisons of constants.
    * 1 > 2
    * ...
    * */
  lazy val ruleDropConstComparison = rule[Exp] {
    case BinaryExp(_: Eq, lhs: Const, rhs: Const)           => BoolConst(lhs == rhs)
    case BinaryExp(_: Neq, lhs: Const, rhs: Const)          => BoolConst(lhs != rhs)
    case BinaryExp(_: Ge, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value >= rhs.value)
    case BinaryExp(_: Ge, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value >= rhs.value)
    case BinaryExp(_: Gt, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value > rhs.value)
    case BinaryExp(_: Gt, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value > rhs.value)
    case BinaryExp(_: Le, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value <= rhs.value)
    case BinaryExp(_: Le, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value <= rhs.value)
    case BinaryExp(_: Lt, lhs: IntConst, rhs: IntConst)     => BoolConst(lhs.value < rhs.value)
    case BinaryExp(_: Lt, lhs: FloatConst, rhs: FloatConst) => BoolConst(lhs.value < rhs.value)
  }

  /** Rules for folding constants across a sequence of additions, multiplications or max.
    * e.g: 1 + (x + 1) => 2 + x
    */

  def merges(m: PrimitiveMonoid, e: Exp): List[Exp] = e match {
    case MergeMonoid(`m`, lhs, rhs) => merges(m, lhs) ++ merges(m, rhs)
    case e                          => List(e)
  }

  def hasNumber(m: NumberMonoid, e: Exp) = merges(m, e).collectFirst{ case _: NumberConst => true }.isDefined

  def splitOnNumbers(m: PrimitiveMonoid, e: Exp) = merges(m, e).partition {
    case _: NumberConst => true
    case _              => false
  }

  def foldConsts(m: PrimitiveMonoid, e1: NumberConst, e2: Exp) = {
    val (consts, rest) = splitOnNumbers(m, e2)
    val const = e1 match {
      case IntConst(v) =>
        IntConst(consts.map{ case IntConst(v) => v }.foldLeft(v)((a,b) => m match {
          case _: SumMonoid => a + b
          case _: MaxMonoid => math.max(a, b)
          case _: MultiplyMonoid => a * b
        }))
      case FloatConst(v) =>
        FloatConst(consts.map{ case FloatConst(v) => v }.foldLeft(v)((a,b) => m match {
          case _: SumMonoid => a + b
          case _: MaxMonoid => math.max(a, b)
          case _: MultiplyMonoid => a * b
        }))
    }
    if (rest.isEmpty) {
      const
    } else {
      val nrhs = rest.tail.foldLeft(rest.head)((a: Exp, b: Exp) => MergeMonoid(m, a, b))
      MergeMonoid(m, const, nrhs)
    }
  }

  // TODO: `hasNumber` followed by `splitOnNumbers` is inefficient
  lazy val ruleFoldConsts = rule[Exp] {
   case MergeMonoid(m: NumberMonoid, lhs: NumberConst, rhs) if hasNumber(m, rhs) => logger.debug("ruleFoldConsts"); foldConsts(m, lhs, rhs)
   case MergeMonoid(m: NumberMonoid, lhs, rhs: NumberConst) if hasNumber(m, lhs) => logger.debug("ruleFoldConsts"); foldConsts(m, rhs, lhs)
  }

}
