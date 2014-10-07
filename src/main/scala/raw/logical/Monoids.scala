package raw.logical

/**
 * Monoid
 */
sealed abstract class Monoid {
  val commutative: Boolean
  val idempotent: Boolean
}

/**
 * PrimitiveMonoid
 */
sealed abstract class PrimitiveMonoid extends Monoid

sealed abstract class NumberMonoid extends PrimitiveMonoid

case object SumMonoid extends NumberMonoid {
  val commutative = true
  val idempotent = false
}

case object MultiplyMonoid extends NumberMonoid {
  val commutative = true
  val idempotent = false 
}

case object MaxMonoid extends NumberMonoid {
  val commutative = true
  val idempotent = true
}

sealed abstract class BoolMonoid extends PrimitiveMonoid

case object OrMonoid extends BoolMonoid {
  val commutative = true
  val idempotent = true
}

case object AndMonoid extends BoolMonoid {
  val commutative = true
  val idempotent = true
}

/**
 * CollectionMonoid
 */
sealed abstract class CollectionMonoid extends Monoid

case object SetMonoid extends CollectionMonoid {
  val commutative = true
  val idempotent = true
}

case object BagMonoid extends CollectionMonoid {
  val commutative = true
  val idempotent = false
} 

case object ListMonoid extends CollectionMonoid {
  val commutative = false
  val idempotent = false
}

/**
 * MonoidPrettyPrinter
 */
object MonoidPrettyPrinter {
  def apply(m: Monoid) = m match {
    case SumMonoid => "sum"
    case MultiplyMonoid => "multiply"
    case MaxMonoid => "max"
    case OrMonoid => "or"
    case AndMonoid => "and"
    case SetMonoid => "set"
    case BagMonoid => "bag"
    case ListMonoid => "list"
  }
}