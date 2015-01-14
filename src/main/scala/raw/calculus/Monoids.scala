package raw.calculus

import org.kiama.util.TreeNode

/** Monoid
 */
sealed abstract class Monoid extends TreeNode {
  val commutative: Boolean
  val idempotent: Boolean

  val mergeSym: String
  val compSym: String

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

  override def toString() = compSym
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

  val mergeSym = "max"
  val compSym = "max"

  // TODO: If the monoid zero is 0, then this monoid only applies to natural numbers?
  //       Shouldn't it be negative infinity?
  def zero = Calculus.IntConst(0)
}

case class MultiplyMonoid() extends NumberMonoid {
  val idempotent = false

  val mergeSym = "*"
  val compSym = "multiply"

  def zero = Calculus.IntConst(1)
}

case class SumMonoid() extends NumberMonoid {
  val idempotent = false

  val mergeSym = "+"
  val compSym = "sum"

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

  val mergeSym = "and"
  val compSym = "and"

  def zero = Calculus.BoolConst(true)
}

case class OrMonoid() extends BoolMonoid {
  val idempotent = true

  val mergeSym = "or"
  val compSym = "or"

  def zero = Calculus.BoolConst(false)
}

/** Collection Monoids
 */
sealed abstract class CollectionMonoid extends Monoid {
  val openSym: String
  val closeSym: String

  def zero = Calculus.ZeroCollectionMonoid(this)
}

case class BagMonoid() extends CollectionMonoid {
  val commutative = true
  val idempotent = false

  val mergeSym = "bag_union"
  val compSym = "bag"
  val openSym = "bag{"
  val closeSym = "}"
}

case class SetMonoid() extends CollectionMonoid {
  val commutative = true
  val idempotent = true

  val mergeSym = "union"
  val compSym = "set"
  val openSym = "{"
  val closeSym = "}"
}

case class ListMonoid() extends CollectionMonoid {
  val commutative = false
  val idempotent = false

  val mergeSym = "append"
  val compSym = "list"
  val openSym = "["
  val closeSym = "]"
}
