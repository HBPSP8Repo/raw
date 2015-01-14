package raw.calculus

trait Simplifier extends Canonizer {

  import CanonicalCalculus._
  import org.kiama.attribution.Attribution._
  import org.kiama.rewriting.Rewriter._

  def simplify(c: Calculus.Comp): Comp = {
    val c1 = canonize(c)
    simplificationRules(c1) match {
      case Some(c2: Comp) => c2
      case _ => c1
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
    ruleDistributeAndOverOr) 

  // TODO: The following use of `orSeq` and `andSeq` does not work.
  // TODO: Must replace by a `def` that returns a set.
  // TODO: Replace uses of `(a->andSeq) contains X` by a pattern match, e.g. a.collect{ case ... }
  // TODO: How to handle `not(a > 5)` being equivalent to `a <= 5` ?

  lazy val orSeq: Exp => Set[Exp] = attr {
    case MergeMonoid(_: OrMonoid, y @ MergeMonoid(_: OrMonoid, _, _), z @ MergeMonoid(_: OrMonoid, _, _)) => (y -> orSeq) ++ (z -> orSeq)
    case MergeMonoid(_: OrMonoid, a, y @ MergeMonoid(_: OrMonoid, _, _))                                  => Set(a) ++ (y -> orSeq)
    case MergeMonoid(_: OrMonoid, y @ MergeMonoid(_: OrMonoid, _, _), b)                                  => (y -> orSeq) ++ Set(b)
    case MergeMonoid(_: OrMonoid, a, b)                                                                   => Set(a, b)
    case _                                                                                                => Set()
  }
   
  lazy val andSeq: Exp => Set[Exp] = attr {
    case MergeMonoid(_: AndMonoid, y @ MergeMonoid(_: AndMonoid, _, _), z @ MergeMonoid(_: AndMonoid, _, _)) => (y -> andSeq) ++ (z -> andSeq)
    case MergeMonoid(_: AndMonoid, a, y @ MergeMonoid(_: AndMonoid, _, _))                                   => Set(a) ++ (y -> andSeq)
    case MergeMonoid(_: AndMonoid, y @ MergeMonoid(_: AndMonoid, _, _), b)                                   => (y -> andSeq) ++ Set(b)
    case MergeMonoid(_: AndMonoid, a, b)                                                                     => Set(a, b)
    case _                                                                                                   => Set()
  }

  /** Rules to simplify Boolean expressions to CNF.
    */

  // true | A => true
  lazy val ruleTrueOrA = rule[Exp] {
    case MergeMonoid(_: OrMonoid, t @ BoolConst(true), _) => t
    case MergeMonoid(_: OrMonoid, _, t @ BoolConst(true)) => t
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
    case MergeMonoid(_: AndMonoid, f @ BoolConst(false), _) => f
    case MergeMonoid(_: AndMonoid, _, f @ BoolConst(false)) => f
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
    case MergeMonoid(_: OrMonoid, a, b) if (b -> orSeq) contains UnaryExp(Not(), a)  => BoolConst(true)
    case MergeMonoid(_: OrMonoid, UnaryExp(_: Not, a), b) if (b -> orSeq) contains a => BoolConst(true)
  }
 
  // A & !A => false
  lazy val ruleAandNotA = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if (b -> andSeq) contains UnaryExp(Not(), a)  => BoolConst(false)
    case MergeMonoid(_: AndMonoid, UnaryExp(_: Not, a), b) if (b -> andSeq) contains a => BoolConst(false)
  }

  // (A | (A | (B | C))) => (A | (B | C))
  lazy val ruleRepeatedOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if (b -> orSeq) contains a => b
  }
 
  // (A & (A & (B & C))) => (A & (B & C))
  lazy val ruleRepeatedAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if (b -> andSeq) contains a => b
  }
 
  // (A & B) | (A & B & C)) => (A & B)
  lazy val ruleRepeatedAndInOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if (a -> andSeq) != Set() && ((a -> andSeq) subsetOf (b -> andSeq)) => a
    case MergeMonoid(_: OrMonoid, a, b) if (b -> andSeq) != Set() && ((b -> andSeq) subsetOf (a -> andSeq)) => b
  } 

  // (A | B) & (A | B | C) => (A | B)
  lazy val ruleRepeateOrInAnd = rule[Exp] {
    case MergeMonoid(_: AndMonoid, a, b) if (a -> orSeq) != Set() && ((a -> orSeq) subsetOf (b -> orSeq)) => a
    case MergeMonoid(_: AndMonoid, a, b) if (b -> orSeq) != Set() && ((b -> orSeq) subsetOf (a -> orSeq)) => b
  } 
 
  /* (P1 & P2 & P3) | (Q1 & Q2 & Q3) =>
   *   (P1 | Q1) & (P1 | Q2) & (P1 | Q3) &
   *   (P2 | Q1) & (P2 | Q2) & (P2 | Q3) &
   *   (P3 | Q1) & (P3 | Q2) & (P3 | Q3) &
   */
  lazy val ruleDistributeAndOverOr = rule[Exp] {
    case MergeMonoid(_: OrMonoid, a, b) if (a -> andSeq) != Set() && (b -> andSeq) != Set() => {
      val prod = for (x <- (a->andSeq); y <- (b->andSeq)) yield MergeMonoid(OrMonoid(), x, y)
      val head = prod.head
      val rest = prod.drop(1)
      rest.foldLeft(head)((a, b) => MergeMonoid(AndMonoid(), a, b))
    }
  }
 
  // (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
  // (Handled by andSeq.)
 
  /** Rules to simplify algebraic expressions.
    */
  
  // x + 0 => x
  lazy val ruleAddZero = rule[Exp] {
    case MergeMonoid(_: SumMonoid, x, IntConst(zero))   if zero == 0 => x
    case MergeMonoid(_: SumMonoid, IntConst(zero), x)   if zero == 0 => x
    case MergeMonoid(_: SumMonoid, x, FloatConst(zero)) if zero == 0 => x
    case MergeMonoid(_: SumMonoid, FloatConst(zero), x) if zero == 0 => x
  }
 
  // x - 0 => x
  lazy val ruleSubZero = rule[Exp] {
    case BinaryExp(_: Sub, x, IntConst(zero))   if zero == 0 => x
    case BinaryExp(_: Sub, x, FloatConst(zero)) if zero == 0 => x
  }

  // TODO: Implement additional math simplification rules.

  // a - b => a + (-b)

  // x - x => 0
    //  x + (-x)
    //
    //  x + -(y + 2)
 
  // --x => x
 
  // x * 0 => 0
  
  // x * 1 => 1
 
  // x / 1 => x
 
  // x / x => 1
 
  // x / (y / z) => x * z / y
  
}