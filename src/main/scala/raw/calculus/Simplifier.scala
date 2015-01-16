package raw.calculus

import raw._

trait Simplifier extends Canonizer {

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
    ruleDivideBySelf)

  def ors(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: OrMonoid, lhs@MergeMonoid(_: OrMonoid, _, _), rhs@MergeMonoid(_: OrMonoid, _, _)) => ors(lhs) ++ ors(rhs)
    case MergeMonoid(_: OrMonoid, lhs, rhs@MergeMonoid(_: OrMonoid, _, _))                                => Set(lhs) ++ ors(rhs)
    case MergeMonoid(_: OrMonoid, lhs@MergeMonoid(_: OrMonoid, _, _), rhs)                                => ors(lhs) ++ Set(rhs)
    case MergeMonoid(_: OrMonoid, lhs, rhs)                                                               => Set(lhs, rhs)
    case _                                                                                                => Set()
  }

  def ands(e: Exp): Set[Exp] = e match {
    case MergeMonoid(_: AndMonoid, lhs@MergeMonoid(_: AndMonoid, _, _), rhs@MergeMonoid(_: AndMonoid, _, _)) => ands(lhs) ++ ands(rhs)
    case MergeMonoid(_: AndMonoid, lhs, rhs@MergeMonoid(_: AndMonoid, _, _))                                 => Set(lhs) ++ ands(rhs)
    case MergeMonoid(_: AndMonoid, lhs@MergeMonoid(_: AndMonoid, _, _), rhs)                                 => ands(lhs) ++ Set(rhs)
    case MergeMonoid(_: AndMonoid, lhs, rhs)                                                                 => Set(lhs, rhs)
    case _                                                                                                   => Set()
  }

  /** Rules to simplify Boolean expressions to CNF.
    */

  // true | A => true
  lazy val ruleTrueOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, t@BoolConst(true), _) => t
    case MergeMonoid(_: OrMonoid, _, t@BoolConst(true)) => t
  }

  // false | A => A
  lazy val ruleFalseOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, BoolConst(false), a) => a
    case MergeMonoid(_: OrMonoid, a, BoolConst(false)) => a
  }

  // true & A => A
  lazy val ruleTrueAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, BoolConst(true), a) => a
    case MergeMonoid(_: AndMonoid, a, BoolConst(true)) => a
  }

  // false & A => false
  lazy val ruleFalseAndA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, f@BoolConst(false), _) => f
    case MergeMonoid(_: AndMonoid, _, f@BoolConst(false)) => f
  }

  // !!A => A
  lazy val ruleNotNotA = rule[Exp] {
    case UnaryExp(_: Not, UnaryExp(_: Not, a)) => a
  }

  // DeMorgan's laws:
  //  !(A & B) => !A | !B
  //  !(A | B) => !A & !B
  lazy val ruleDeMorgan = rule[Exp] {
    case UnaryExp(_: Not, MergeMonoid(_: AndMonoid, a, b)) => MergeMonoid(OrMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
    case UnaryExp(_: Not, MergeMonoid(_: OrMonoid, a, b))  => MergeMonoid(AndMonoid(), UnaryExp(Not(), a), UnaryExp(Not(), b))
  }

  // A | !A => true
  lazy val ruleAorNotA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains UnaryExp(Not(), a)  => BoolConst(true)
    case MergeMonoid(_: OrMonoid, UnaryExp(_: Not, a), b) if ors(b) contains a => BoolConst(true)
  }

  // A & !A => false
  lazy val ruleAandNotA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains UnaryExp(Not(), a)  => BoolConst(false)
    case MergeMonoid(_: AndMonoid, UnaryExp(_: Not, a), b) if ands(b) contains a => BoolConst(false)
  }

  // (A | (A | (B | C))) => (A | (B | C))
  lazy val ruleRepeatedOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if ors(b) contains a => b
  }

  // (A & (A & (B & C))) => (A & (B & C))
  lazy val ruleRepeatedAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if ands(b) contains a => b
  }

  // (A & B) | (A & B & C)) => (A & B)
  lazy val ruleRepeatedAndInOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if !ands(a).isEmpty && (ands(a) subsetOf ands(b)) => a
    case MergeMonoid(_: OrMonoid, a, b) if !ands(b).isEmpty && (ands(b) subsetOf ands(a)) => b
  }

  // (A | B) & (A | B | C) => (A | B)
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

  // (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
  // (Handled by `ands`.)

  /** Rules to simplify algebraic expressions.
    */

  // TODO: Add rules to fold/merge all constants across additions and multiplications.
  // TODO: Take care w/ type system (to not overcast?).

  // x + 0 => x
  lazy val ruleAddZero = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, IntConst(v)) if v == 0   => lhs
    case MergeMonoid(_: SumMonoid, IntConst(v), rhs) if v == 0   => rhs
    case MergeMonoid(_: SumMonoid, lhs, FloatConst(v)) if v == 0 => lhs
    case MergeMonoid(_: SumMonoid, FloatConst(v), rhs) if v == 0 => rhs
  }

  // x - 0 => x
  lazy val ruleSubZero = rule[Exp] {
    case BinaryExp(_: Sub, lhs, IntConst(v)) if v == 0   => lhs
    case BinaryExp(_: Sub, lhs, FloatConst(v)) if v == 0 => lhs
  }

  // a - b => a + (-b)
  lazy val ruleReplaceSubByNeg = rule[Exp] {
    case BinaryExp(_: Sub, lhs, rhs) => MergeMonoid(SumMonoid(), lhs, UnaryExp(Neg(), rhs))
  }

  // x + (-x) => 0
  lazy val ruleSubSelf = rule[Exp] {
    case MergeMonoid(_: SumMonoid, lhs, UnaryExp(_: Neg, rhs)) if lhs == rhs => IntConst(0)
  }

  // --x => x
  lazy val ruleRemoveDoubleNeg = rule[Exp] {
    case UnaryExp(_: Neg, UnaryExp(_: Neg, e)) => e
  }

  // x * 0 => 0
  lazy val ruleMultiplyByZero = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, c@IntConst(v)) if v == 0   => c
    case MergeMonoid(_: MultiplyMonoid, c@IntConst(v), rhs) if v == 0   => c
    case MergeMonoid(_: MultiplyMonoid, lhs, c@FloatConst(v)) if v == 0 => c
    case MergeMonoid(_: MultiplyMonoid, c@FloatConst(v), rhs) if v == 0 => c
  }

  // x * 1 => x
  lazy val ruleMultiplyByOne = rule[Exp] {
    case MergeMonoid(_: MultiplyMonoid, lhs, c@IntConst(v)) if v == 1   => lhs
    case MergeMonoid(_: MultiplyMonoid, c@IntConst(v), rhs) if v == 1   => rhs
    case MergeMonoid(_: MultiplyMonoid, lhs, c@FloatConst(v)) if v == 1 => lhs
    case MergeMonoid(_: MultiplyMonoid, c@FloatConst(v), rhs) if v == 1 => rhs
  }

  // x / 1 => x
  lazy val ruleDivideByOne = rule[Exp] {
    case BinaryExp(_: Div, lhs, c@IntConst(v)) if v == 1   => lhs
    case BinaryExp(_: Div, lhs, c@FloatConst(v)) if v == 1 => lhs
  }

  // x / x => 1
  lazy val ruleDivideBySelf = rule[Exp] {
    case BinaryExp(_: Div, lhs, rhs) if lhs == rhs => IntConst(1)
  }

  // x / (y / z) => x * z / y
  // TODO: Implement since it adds opportunities for simplication
//  lazy val ruleDivDivByMultDiv = rule[Exp] {
//
//  }

}