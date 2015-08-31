package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

object Constraint extends LazyLogging {

  import raw.calculus.Calculus.Exp

  sealed abstract class Constraint extends RawNode

  case class And(cs: Constraint*) extends Constraint

  case class Or(cs: Constraint*) extends Constraint

  case class SameType(e1: Exp, e2: Exp, desc: Option[String] = None) extends Constraint

  case class HasType(e: Exp, expected: Type, desc: Option[String] = None) extends Constraint

  case object NoConstraint extends Constraint

}