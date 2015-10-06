package raw
package calculus

import raw.calculus.Calculus.{Bind, Gen}

import scala.collection.immutable.Seq

import com.typesafe.scalalogging.LazyLogging

object Constraint extends LazyLogging {

  import Calculus._

  sealed abstract class Constraint extends RawNode

  case class SameType(e1: Exp, e2: Exp, desc: Option[String] = None) extends Constraint

  case class HasType(e: Exp, expected: Type, desc: Option[String] = None) extends Constraint

  case class ExpMonoidSubsetOf(e: Exp, m: Monoid) extends Constraint

  case class InheritsOption(t: Type, ts: Seq[Type]) extends Constraint

  case class PartitionHasType(p: Partition) extends Constraint

  case class StarHasType(s: Star) extends Constraint

  case class MaxOfMonoids(e: Exp, gs: Seq[Gen]) extends Constraint

  case class BoundByType(b: Bind) extends Constraint

  case class IdnIsDefined(idnExp: IdnExp) extends Constraint

  case class GenPatternHasType(g: Gen) extends Constraint

  case class FunAppType(f: FunApp) extends Constraint
}