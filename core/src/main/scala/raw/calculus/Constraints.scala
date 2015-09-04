package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

object Constraint extends LazyLogging {

  sealed abstract class Constraint extends RawNode

  case class And(cs: Constraint*) extends Constraint

  case class SameType(t1: Type, t2: Type, desc: Option[String] = None) extends Constraint

  case class HasType(t: Type, expected: Type, desc: Option[String] = None) extends Constraint

  case object NoConstraint extends Constraint

}