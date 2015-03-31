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

  sealed abstract class SparkNode extends PhysicalAlgebraNode

  sealed abstract class ScalaNode extends PhysicalAlgebraNode

  case class ScalaScan(name: String, t: Type) extends ScalaNode
  case class SparkScan(name: String, t: Type) extends SparkNode
  
  case class ScalaReduce(m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends ScalaNode
  case class SparkReduce(m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends SparkNode

  case class ScalaNest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends ScalaNode
  case class SparkNest(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends SparkNode

  case class ScalaSelect(p: Exp, child: PhysicalAlgebraNode) extends ScalaNode
  case class SparkSelect(p: Exp, child: PhysicalAlgebraNode) extends SparkNode

  case class ScalaJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode
  case class SparkJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode

  case class ScalaUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaNode
  case class SparkUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkNode

  case class ScalaOuterJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode
  case class SparkOuterJoin(p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode

  case class ScalaOuterUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaNode
  case class SparkOuterUnnest(path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkNode

  case class ScalaMerge(m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode
  case class SparkMerge(m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode

  case class ScalaToSparkNode(node:ScalaNode) extends SparkNode
}

