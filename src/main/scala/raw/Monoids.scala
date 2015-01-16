package raw

import org.kiama.util.TreeNode

/** Monoid
 */
sealed abstract class Monoid extends TreeNode {
  val commutative: Boolean
  val idempotent: Boolean

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
  val commutative = true

  def isOfType(t: Type): Boolean
}

sealed abstract class NumberMonoid extends PrimitiveMonoid {
  def isOfType(t: Type) = t match {
    case _: IntType | _: FloatType => true
    case _                         => false
  }
}

case class MaxMonoid() extends NumberMonoid {
  val idempotent = true
}

case class MultiplyMonoid() extends NumberMonoid {
  val idempotent = false
}

case class SumMonoid() extends NumberMonoid {
  val idempotent = false
}

sealed abstract class BoolMonoid extends PrimitiveMonoid {
  def isOfType(t: Type) = t match {
    case _: BoolType => true
    case _           => false
  }
}

case class AndMonoid() extends BoolMonoid {
  val idempotent = true
}

case class OrMonoid() extends BoolMonoid {
  val idempotent = true
}

/** Collection Monoids
 */
sealed abstract class CollectionMonoid extends Monoid

case class BagMonoid() extends CollectionMonoid {
  val commutative = true
  val idempotent = false
}

case class SetMonoid() extends CollectionMonoid {
  val commutative = true
  val idempotent = true
}

case class ListMonoid() extends CollectionMonoid {
  val commutative = false
  val idempotent = false
}
