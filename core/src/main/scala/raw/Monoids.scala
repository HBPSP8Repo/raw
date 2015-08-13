package raw

/** Monoid
  */
sealed abstract class Monoid extends RawNode {
  def commutative: Boolean
  def idempotent: Boolean
}

/** Primitive Monoids
  */
sealed abstract class PrimitiveMonoid extends Monoid {
  def commutative = true
}

sealed abstract class NumberMonoid extends PrimitiveMonoid
case class MaxMonoid() extends NumberMonoid {
  def idempotent = true
}

case class MinMonoid() extends NumberMonoid {
  def idempotent = true
}

case class MultiplyMonoid() extends NumberMonoid {
  def idempotent = false
}

case class SumMonoid() extends NumberMonoid {
  def idempotent = false
}

sealed abstract class BoolMonoid extends PrimitiveMonoid

case class AndMonoid() extends BoolMonoid {
  def idempotent = true
}

case class OrMonoid() extends BoolMonoid {
  def idempotent = true
}

/** Collection Monoids
  */
sealed abstract class CollectionMonoid extends Monoid

case class BagMonoid() extends CollectionMonoid {
  def commutative = true
  def idempotent = false
}

case class SetMonoid() extends CollectionMonoid {
  def commutative = true
  def idempotent = true
}

case class ListMonoid() extends CollectionMonoid {
  def commutative = false
  def idempotent = false
}
