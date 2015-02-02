package raw

/** Monoid
  */
sealed abstract class Monoid extends RawNode {
  def commutative: Boolean
  def idempotent: Boolean

  def greaterOrEqThan(other: Monoid): Boolean =
    if (commutative && idempotent)
      true
    else if (commutative && !idempotent)
      !other.idempotent
    else if (!commutative && idempotent)
      !other.commutative
    else
      !other.commutative && !other.idempotent
}

/** Primitive Monoids
  */
sealed abstract class PrimitiveMonoid extends Monoid {
  def commutative = true

  def isOfType(t: Type): Boolean
}

sealed abstract class NumberMonoid extends PrimitiveMonoid {
  def isOfType(t: Type) = t match {
    case _: IntType | _: FloatType => true
    case _                         => false
  }
}

case class MaxMonoid() extends NumberMonoid {
  def idempotent = true
}

case class MultiplyMonoid() extends NumberMonoid {
  def idempotent = false
}

case class SumMonoid() extends NumberMonoid {
  def idempotent = false
}

sealed abstract class BoolMonoid extends PrimitiveMonoid {
  def isOfType(t: Type) = t match {
    case _: BoolType => true
    case _           => false
  }
}

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
