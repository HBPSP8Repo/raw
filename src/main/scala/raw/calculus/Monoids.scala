package raw.calculus

import scala.util.parsing.input.Positional

/** Monoid
 */

sealed abstract class Monoid extends Positional {
  /** Returns true if monoid has type 't' */
  def hasType(t: MonoidType): Boolean  
  
  def commutative: Boolean

  def idempotent: Boolean
}

/** PrimitiveMonoid
 */

sealed abstract class PrimitiveMonoid extends Monoid

sealed abstract class IntMonoid extends PrimitiveMonoid {
  def hasType(t: MonoidType) = t == IntType
}

case class SumMonoid extends IntMonoid {
  def commutative = true
    
  def idempotent = false
}

case class MultiplyMonoid extends IntMonoid {
  def commutative = true
  
  def idempotent = false 
}

case class MaxMonoid extends IntMonoid {
  def commutative = true
  
  def idempotent = true
}

sealed abstract class BoolMonoid extends PrimitiveMonoid {
  def hasType(t: MonoidType) = t == BoolType
}

case class OrMonoid extends BoolMonoid {
  def commutative = true
  
  def idempotent = true
}

case class AndMonoid extends BoolMonoid {
  def commutative = true
  
  def idempotent = true
}

/** CollectionMonoid
 */

sealed abstract class CollectionMonoid extends Monoid

case class SetMonoid extends CollectionMonoid {
  def hasType(t: MonoidType) = t match {
    case SetType(_) => true
    case _ => false
  }
  
  def commutative = true
  
  def idempotent = true
}

case class BagMonoid extends CollectionMonoid {
  def hasType(t: MonoidType) = t match {
    case BagType(_) => true
    case _ => false
  }

  def commutative = true
  
  def idempotent = false
} 

case class ListMonoid extends CollectionMonoid {
  def hasType(t: MonoidType) = t match {
    case ListType(_) => true
    case _ => false
  }
  
  def commutative = false
  
  def idempotent = false
}

/** MonoidPrettyPrinter
 */
object MonoidPrettyPrinter {
  def apply(m: Monoid) = m match {
    case SumMonoid() => "sum"
    case MultiplyMonoid() => "multiply"
    case MaxMonoid() => "max"
    case OrMonoid() => "or"
    case AndMonoid() => "and"
    case SetMonoid() => "set"
    case BagMonoid() => "bag"
    case ListMonoid() => "list"
  }
}