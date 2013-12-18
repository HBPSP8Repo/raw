package raw.algebra

/** Monoid
 */

sealed abstract class Monoid

/** PrimitiveMonoid
 */

sealed abstract class PrimitiveMonoid extends Monoid

case object SumMonoid extends PrimitiveMonoid

case object MultiplyMonoid extends PrimitiveMonoid

case object MaxMonoid extends PrimitiveMonoid

case object OrMonoid extends PrimitiveMonoid

case object AndMonoid extends PrimitiveMonoid

/** CollectionMonoid
 */

sealed abstract class CollectionMonoid extends Monoid

case object SetMonoid extends CollectionMonoid

case object BagMonoid extends CollectionMonoid

case object ListMonoid extends CollectionMonoid

/** MonoidPrettyPrinter
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