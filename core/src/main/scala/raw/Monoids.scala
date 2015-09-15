package raw

import raw.calculus.{SymbolTable, Symbol}

/** Monoid
  */
sealed abstract class Monoid extends RawNode {
  def commutative: Option[Boolean]
  def idempotent: Option[Boolean]
}

/** Primitive Monoids
  */
sealed abstract class PrimitiveMonoid extends Monoid {
  def commutative = Some(true)
}

sealed abstract class NumberMonoid extends PrimitiveMonoid
case class MaxMonoid() extends NumberMonoid {
  def idempotent = Some(true)
}

case class MinMonoid() extends NumberMonoid {
  def idempotent = Some(true)
}

case class MultiplyMonoid() extends NumberMonoid {
  def idempotent = Some(false)
}

case class SumMonoid() extends NumberMonoid {
  def idempotent = Some(false)
}

sealed abstract class BoolMonoid extends PrimitiveMonoid

case class AndMonoid() extends BoolMonoid {
  def idempotent = Some(true)
}

case class OrMonoid() extends BoolMonoid {
  def idempotent = Some(true)
}

/** Collection Monoids
  */
sealed abstract class CollectionMonoid extends Monoid

case class BagMonoid() extends CollectionMonoid {
  def commutative = Some(true)
  def idempotent = Some(false)
}

case class SetMonoid() extends CollectionMonoid {
  def commutative = Some(true)
  def idempotent = Some(true)
}

case class ListMonoid() extends CollectionMonoid {
  def commutative = Some(false)
  def idempotent = Some(false)
}

/** Monoid Variable
  */
case class MonoidVariable(commutative: Option[Boolean] = None, idempotent: Option[Boolean] = None, sym: Symbol = SymbolTable.next()) extends CollectionMonoid