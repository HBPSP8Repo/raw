package raw.calculus

import scala.util.parsing.input.Positional

/** Monoid
 */

sealed abstract class Monoid extends Positional {
  def commutative: Boolean

  def idempotent: Boolean
}

/** PrimitiveMonoid
 */

sealed abstract class PrimitiveMonoid extends Monoid

sealed abstract class NumberMonoid extends PrimitiveMonoid

case class SumMonoid extends NumberMonoid {
  def commutative = true
    
  def idempotent = false
}

case class MultiplyMonoid extends NumberMonoid {
  def commutative = true
  
  def idempotent = false 
}

case class MaxMonoid extends NumberMonoid {
  def commutative = true
  
  def idempotent = true
}

sealed abstract class BoolMonoid extends PrimitiveMonoid

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
  def commutative = true
  
  def idempotent = true
}

case class BagMonoid extends CollectionMonoid {
  def commutative = true
  
  def idempotent = false
} 

case class ListMonoid extends CollectionMonoid {
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