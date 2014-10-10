package raw

import org.kiama.attribution._
import org.kiama.attribution.Attribution._
import org.kiama.rewriting.Rewriter._
import raw._
import raw.logical._
import raw.logical.calculus._

object KiamaScratchpad extends App {
  println("Welcome to the Kiama")

  //  val tree = MergeMonoid(BoolType, AndMonoid, BoolConst(true), BoolConst(false))
  //	val bool_reduction = rule[Expression] {
  //	  case MergeMonoid(BoolType, AndMonoid, _, BoolConst(false)) => BoolConst(false)
  //	}
  //	outermost (bool_reduction) (tree) match {
  //		case Some(t : Expression) => println("new->" + CalculusPrettyPrinter(t))
  //		case _ => println("nothing...") 
  //	}

  val varA = Variable (BoolType, "a", "a")
  val varB = Variable (BoolType, "b", "b")
  val varC = Variable (BoolType, "c", "c")
  val varD = Variable (BoolType, "d", "d")
  val varE = Variable (BoolType, "e", "e")
  
  //val tree = MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varB, varC)))
  //val tree = MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varB, Not(varA)))
  //val tree = MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varB, Not(varA))))
  //Attribution.initTree (tree)

	/**
	 * Rules to simplify Boolean expressions to CNF.
	 */

  val orSeq : Expression => Set[TypedExpression] =
    attr {
      case MergeMonoid (BoolType, OrMonoid, y @ MergeMonoid (BoolType, OrMonoid, _, _), z @ MergeMonoid (BoolType, OrMonoid, _, _)) => (y->orSeq) ++ (z->orSeq)
      case MergeMonoid (BoolType, OrMonoid, a, y @ MergeMonoid (BoolType, OrMonoid, _, _))                                          => Set(a) ++ (y->orSeq)
      case MergeMonoid (BoolType, OrMonoid, y @ MergeMonoid (BoolType, OrMonoid, _, _), b)                                          => (y->orSeq) ++ Set(b)
      case MergeMonoid (BoolType, OrMonoid, a, b) 																				                                          => Set(a, b)
      case _                                      																				                                          => Set()
    }
    
  val andSeq : Expression => Set[TypedExpression] =
    attr {
      case MergeMonoid (BoolType, AndMonoid, y @ MergeMonoid (BoolType, AndMonoid, _, _), z @ MergeMonoid (BoolType, AndMonoid, _, _)) => (y->andSeq) ++ (z->andSeq)
      case MergeMonoid (BoolType, AndMonoid, a, y @ MergeMonoid (BoolType, AndMonoid, _, _))                                           => Set(a) ++ (y->andSeq)
      case MergeMonoid (BoolType, AndMonoid, y @ MergeMonoid (BoolType, AndMonoid, _, _), b)                                           => (y->andSeq) ++ Set(b)
      case MergeMonoid (BoolType, AndMonoid, a, b) 																				                                             => Set(a, b)
      case _                                      																				                                             => Set()
    }

  // true | A => true
  val ruleTrueOrA = rule[Expression] {
    case MergeMonoid (BoolType, OrMonoid, t @ BoolConst(true), _) => t
    case MergeMonoid (BoolType, OrMonoid, _, t @ BoolConst(true)) => t
  }

  // false | A => A
  val ruleFalseOrA = rule[Expression] {
    case MergeMonoid (BoolType, OrMonoid, BoolConst(false), a) => a
    case MergeMonoid (BoolType, OrMonoid, a, BoolConst(false)) => a
  }
  
  // true & A => A
  val ruleTrueAndA = rule[Expression] {
    case MergeMonoid (BoolType, AndMonoid, BoolConst(true), a) => a
    case MergeMonoid (BoolType, AndMonoid, a, BoolConst(true)) => a
  }
  
  // false & A => false
  val ruleFalseAndA = rule[Expression] {
    case MergeMonoid (BoolType, AndMonoid, f @ BoolConst(false), _) => f
    case MergeMonoid (BoolType, AndMonoid, _, f @ BoolConst(false)) => f
  }  

  // !!A => A
  val ruleNotNotA = rule[Expression] {
    case Not(Not(a)) => a
  }

	// DeMorgan's laws:
	//  !(A & B) => !A | !B
	//  !(A | B) => !A & !B
  val ruleDeMorgan = rule[Expression] {
		case Not(MergeMonoid(t, AndMonoid, a, b)) => MergeMonoid(t, OrMonoid, Not(a), Not(b))
		case Not(MergeMonoid(t, OrMonoid, a, b))  => MergeMonoid(t, AndMonoid, Not(a), Not(b))
	}

  // A | !A => true
  val ruleAorNotA = rule[Expression] {
    case MergeMonoid (BoolType, OrMonoid, a, b) if (b->orSeq) contains Not(a) => BoolConst(true)
    case MergeMonoid (BoolType, OrMonoid, Not(a), b) if (b->orSeq) contains a => BoolConst(true)
  }
  
  // A & !A => false
  val ruleAandNotA = rule[Expression] {
    case MergeMonoid (BoolType, AndMonoid, a, b) if (b->orSeq) contains Not(a) => BoolConst(false)
    case MergeMonoid (BoolType, AndMonoid, Not(a), b) if (b->orSeq) contains a => BoolConst(false)
  }

  // (A | (A | (B | C))) => (A | (B | C))
  val ruleRepeatedOr = rule[Expression] {
    case MergeMonoid (BoolType, OrMonoid, a, b) if (b->orSeq) contains a => b
  }
  
	// (A & (A & (B & C))) => (A & (B & C))
  val ruleRepeatedAnd = rule[Expression] {
    case MergeMonoid (BoolType, AndMonoid, a, b) if (b->andSeq) contains a => b
  }
  
  // (A & B) | (A & B & C)) => (A & B)
  val ruleRepeatedAndInOr = rule[Expression] {
    case MergeMonoid (BoolType, OrMonoid, a, b) if (a->andSeq) != Set() && ((a->andSeq) subsetOf (b->andSeq)) => a
    case MergeMonoid (BoolType, OrMonoid, a, b) if (b->andSeq) != Set() && ((b->andSeq) subsetOf (a->andSeq)) => b
  } 

  // (A | B) & (A | B | C) => (A | B)
  val ruleRepeateOrInAnd = rule[Expression] {
    case MergeMonoid (BoolType, AndMonoid, a, b) if (a->orSeq) != Set() && ((a->orSeq) subsetOf (b->orSeq)) => a
    case MergeMonoid (BoolType, AndMonoid, a, b) if (b->orSeq) != Set() && ((b->orSeq) subsetOf (a->orSeq)) => b
  } 
  
	/* (P1 & P2 & P3) | (Q1 & Q2 & Q3) =>
	 *   (P1 | Q1) & (P1 | Q2) & (P1 | Q3) &
	 *   (P2 | Q1) & (P2 | Q2) & (P2 | Q3) &
	 *   (P3 | Q1) & (P3 | Q2) & (P3 | Q3) &
	 */
	val ruleDistributeAndOverOr = rule[Expression] {
		case MergeMonoid (BoolType, OrMonoid, a, b) if (a->andSeq) != Set() && (b->andSeq) != Set() =>
		  for (x <- (a->andSeq); y <- (b->andSeq))
		   println("-> " + x + "  " + y);
		  val prod = for (x <- (a->andSeq); y <- (b->andSeq))
		      yield MergeMonoid (BoolType, OrMonoid, x, y)
		  println(prod)
		      
		  val head = prod.head
			val rest = prod.drop(1)
      rest.foldLeft(head)((a, b) => MergeMonoid(BoolType, AndMonoid, a, b))
	}
  
  // (P1 & P2 & P3) & (Q1 & Q2 & Q3) => P1 & P2 & P3 & Q1 & Q2 & Q3
  // (Handled by andSeq)
  
	val rules = reduce(
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

  //val tree = MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varA, MergeMonoid (BoolType, OrMonoid, varB, Not(varA))))
  
  val tree = MergeMonoid (BoolType, OrMonoid, MergeMonoid( BoolType, AndMonoid, varA, varB), MergeMonoid(BoolType, AndMonoid, varC, MergeMonoid(BoolType, AndMonoid, varD, varE)))
  
  Attribution.initTree (tree)

	//println( bottomup (rules) (tree))

	def simplify (t : Expression): Expression = {
		bottomup (rules) (t) match {
			case Some (nt : Expression) => if (t == nt) t else { println(CalculusPrettyPrinter(nt)); simplify(nt) }
		}
  }
	println(CalculusPrettyPrinter(tree))
	
	val simplified = simplify (tree)
	println (simplified)
  println (CalculusPrettyPrinter (simplified))
  
  /**
   * Rules to simplify algebraic expressions.
   */
   
  
   
  // x + 0 => x
  val ruleAddZero = rule[Expression] {
    case BinaryOperation(_, Add, x, IntConst(0))   => x
    case BinaryOperation(_, Add, IntConst(0), x)   => x    
    case BinaryOperation(_, Add, x, FloatConst(0)) => x
    case BinaryOperation(_, Add, FloatConst(0), x) => x
  }
  
  // x - 0 => x
  val ruleSubZero = rule[Expression] {
    case BinaryOperation(_, Sub, x, IntConst(0))   => x
    case BinaryOperation(_, Sub, x, FloatConst(0)) => x
  }
  
  // a - b => a + (-b)
  
  // x - x => 0
  x + (-x)
  
  x + -(y + 2)
  
  // --x => x
  
  // x * 0 => 0
  
  // x * 1 => 1
  
  // x / 1 => x
  
  // x / x => 1
  
  // x / (y / z) => x * z / y
  
  // 
  
  

}