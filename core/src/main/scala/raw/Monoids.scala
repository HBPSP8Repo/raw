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

/** Monoid Variable
  * Monoids have a relative ordering between them: e.g. list < bag < set.
  * lesserMonoids are the ones that are "smaller" than the monoid represented by the monoid variable.
  * Therefore, the monoid variable represents a monoid that is greater or equal to all the lesserMonoids.
  */
// TODO: Rename to CollectionMonoidVariable
// TODO: Remove leq and geq notions
case class MonoidVariable(sym: Symbol = SymbolTable.next()) extends CollectionMonoid

case class GenericMonoid(commutative: Option[Boolean] = None, idempotent: Option[Boolean] = None) extends CollectionMonoid
