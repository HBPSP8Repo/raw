package raw.logical.calculus.parser

import scala.util.parsing.input.Positional

/**
 * Monoid
 *  
 * Monoids are redefined here as classes instead of objects since they inherit from Positional.  
 */
sealed abstract class Monoid extends Positional {
  val commutative: Boolean

  val idempotent: Boolean
}

/**
 * PrimitiveMonoid
 */
sealed abstract class PrimitiveMonoid extends Monoid

sealed abstract class NumberMonoid extends PrimitiveMonoid

case class SumMonoid() extends NumberMonoid {
  val commutative = true
    
  val idempotent = false
}

case class MultiplyMonoid() extends NumberMonoid {
  val commutative = true
  
  val idempotent = false 
}

case class MaxMonoid() extends NumberMonoid {
  val commutative = true
  
  val idempotent = true
}

sealed abstract class BoolMonoid extends PrimitiveMonoid

case class OrMonoid() extends BoolMonoid {
  val commutative = true
  
  val idempotent = true
}

case class AndMonoid() extends BoolMonoid {
  val commutative = true
  
  val idempotent = true
}

/**
 * CollectionMonoid
 */
sealed abstract class CollectionMonoid extends Monoid

case class SetMonoid() extends CollectionMonoid {
  val commutative = true
  
  val idempotent = true
}

case class BagMonoid() extends CollectionMonoid {
  val commutative = true
  
  val idempotent = false
} 

case class ListMonoid() extends CollectionMonoid {
  val commutative = false
  
  val idempotent = false
}

/**
 * MonoidPrettyPrinter
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