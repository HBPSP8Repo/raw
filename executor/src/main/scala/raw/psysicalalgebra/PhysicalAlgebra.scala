package raw.psysicalalgebra

import raw.algebra.Expressions.Exp
import raw.{Type, Monoid, RawNode}
import raw.algebra.AlgebraNode
// Converters ScalaNode <-> SparkNode
object PhysicalAlgebra {

  import org.kiama.relation.Tree

  /** Tree type for Calculus
    */
  type Algebra = Tree[RawNode, PhysicalAlgebraNode]

  sealed abstract class PhysicalAlgebraNode extends AlgebraNode

  sealed abstract class SparkPhysicalAlgebraNode extends PhysicalAlgebraNode

  sealed abstract class ScalaPhysicalAlgebraNode extends PhysicalAlgebraNode

  case class ScalaScan(name: String, t: Type) extends ScalaPhysicalAlgebraNode
  case class SparkScan(name: String, t: Type) extends SparkPhysicalAlgebraNode
  
  case class ScalaReduce(m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkReduce(m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaNest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkNest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaSelect(p: Exp, child: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkSelect(p: Exp, child: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaOuterJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkOuterJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaOuterUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkOuterUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

  case class ScalaMerge(m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaPhysicalAlgebraNode
  case class SparkMerge(m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkPhysicalAlgebraNode

}

