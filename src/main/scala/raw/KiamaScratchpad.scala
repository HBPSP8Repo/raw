package raw

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
//		case Some(t : Expression) => println("new -> " + CalculusPrettyPrinter(t))
//		case _ => println("nothing...") 
//	}
	
	val tree = MergeMonoid(BoolType, AndMonoid, BoolConst(true), BoolConst(false))
	val rule1 = rule[Expression] {
	  case MergeMonoid(BoolType, AndMonoid, _, BoolConst(false)) => BoolConst(false)
	}
	
	val rule2 = rule[Expression] {
	  // (A | (A | (B | C))) => (A | (B | C))
	  case MergeMonoid(BoolType, OrMonoid, a, rhs) if flatten(rhs) contains a => rhs 
	}
	
	outermost (bool_reduction) (tree) match {
		case Some(t : Expression) => println("new -> " + CalculusPrettyPrinter(t))
		case _ => println("nothing...") 
	}
	
}