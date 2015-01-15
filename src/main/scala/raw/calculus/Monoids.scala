package raw.calculus

import org.kiama.util.TreeNode

/** Monoid
 */
sealed abstract class Monoid extends TreeNode {
  val commutative: Boolean
  val idempotent: Boolean

  def zero: Calculus.Exp

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

  // TODO: If the monoid zero is 0, then this monoid only applies to natural numbers?
  // TODO: Shouldn't it be negative infinity?
  def zero = Calculus.IntConst(0)
}

case class MultiplyMonoid() extends NumberMonoid {
  val idempotent = false

  def zero = Calculus.IntConst(1)
}

case class SumMonoid() extends NumberMonoid {
  val idempotent = false

  def zero = Calculus.IntConst(0)
}

sealed abstract class BoolMonoid extends PrimitiveMonoid {
  def isOfType(t: Type) = t match {
    case _: BoolType => true
    case _           => false
  }
}

case class AndMonoid() extends BoolMonoid {
  val idempotent = true

  def zero = Calculus.BoolConst(true)
}

case class OrMonoid() extends BoolMonoid {
  val idempotent = true

  def zero = Calculus.BoolConst(false)
}

/** Collection Monoids
 */
sealed abstract class CollectionMonoid extends Monoid {
  def zero = Calculus.ZeroCollectionMonoid(this)
}

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
