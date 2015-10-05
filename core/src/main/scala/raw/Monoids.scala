package raw

import raw.calculus.{SymbolTable, Symbol}

import scala.collection.immutable.Set

/** Monoid
  */
sealed abstract class Monoid extends RawNode

/** Primitive Monoids
  */
sealed abstract class PrimitiveMonoid extends Monoid

sealed abstract class NumberMonoid extends PrimitiveMonoid

case class MaxMonoid() extends NumberMonoid

case class MinMonoid() extends NumberMonoid

case class MultiplyMonoid() extends NumberMonoid

case class SumMonoid() extends NumberMonoid

sealed abstract class BoolMonoid extends PrimitiveMonoid

case class AndMonoid() extends BoolMonoid

case class OrMonoid() extends BoolMonoid

/** Collection Monoids
  */
sealed abstract class CollectionMonoid extends Monoid

case class BagMonoid() extends CollectionMonoid

case class SetMonoid() extends CollectionMonoid

case class ListMonoid() extends CollectionMonoid

/** Generic Monoid
  * Used only to report the final monoid
  */
case class GenericMonoid(commutative: Option[Boolean] = None, idempotent: Option[Boolean] = None, sym: Symbol = SymbolTable.next()) extends CollectionMonoid

/** Monoid Variable
  */
// TODO: Rename to CollectionMonoidVariable
case class MonoidVariable(ms: Set[CollectionMonoid] = Set(), sym: Symbol = SymbolTable.next()) extends CollectionMonoid