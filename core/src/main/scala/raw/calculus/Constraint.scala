package raw
package calculus

import raw.calculus.Calculus.{Bind, Gen}

import scala.collection.immutable.Seq

import com.typesafe.scalalogging.LazyLogging

object Constraint extends LazyLogging {

  import Calculus.{Exp, Select}

  sealed abstract class Constraint extends RawNode

  case class SameType(e1: Exp, e2: Exp, desc: Option[String] = None) extends Constraint

  case class HasType(e: Exp, expected: Type, desc: Option[String] = None) extends Constraint

  case class ExpMonoidSubsetOf(e: Exp, m: Monoid) extends Constraint

  case class InheritsOption(t: Type, ts: Seq[Type]) extends Constraint

  case class PartitionHasType(s: Select) extends Constraint

  case class MaxOfMonoids(s: Select) extends Constraint

  case class InheritType(b: Bind) extends Constraint

}