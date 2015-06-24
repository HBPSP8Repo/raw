package raw.psysicalalgebra

import raw.algebra.Expressions.Exp
import raw.algebra.LogicalAlgebra._
import raw.{Type, Monoid, RawNode}
import raw.algebra.AlgebraNode
// Converters ScalaNode <-> SparkNode
object PhysicalAlgebra {

  import org.kiama.relation.Tree

  /** Tree type for Calculus
    */
  type Algebra = Tree[RawNode, PhysicalAlgebraNode]

  sealed abstract class PhysicalAlgebraNode(val logicalNode:LogicalAlgebraNode) extends AlgebraNode

  sealed abstract class SparkNode(logicalNode:LogicalAlgebraNode) extends PhysicalAlgebraNode(logicalNode)

  sealed abstract class ScalaNode(logicalNode:LogicalAlgebraNode) extends PhysicalAlgebraNode(logicalNode)

  case class ScalaScan(logicalScan:Scan, name: String, t: Type) extends ScalaNode(logicalScan)
  case class SparkScan(logicalScan:Scan, name: String, t: Type) extends SparkNode(logicalScan)
  
  case class ScalaReduce(logicalReduce:Reduce, m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends ScalaNode(logicalReduce)
  case class SparkReduce(logicalReduce:Reduce, m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode) extends SparkNode(logicalReduce)

  case class ScalaNest(logicalNest:Nest, m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends ScalaNode(logicalNest)
  case class SparkNest(logicalNest:Nest, m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode) extends SparkNode(logicalNest)

  case class ScalaSelect(logicalSelect:Select, p: Exp, child: PhysicalAlgebraNode) extends ScalaNode(logicalSelect)
  case class SparkSelect(logicalSelect:Select, p: Exp, child: PhysicalAlgebraNode) extends SparkNode(logicalSelect)

  case class ScalaJoin(logicalJoin:Join, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode(logicalJoin)
  case class SparkJoin(logicalJoin:Join, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode(logicalJoin)

  case class ScalaUnnest(logicalUnnest:Unnest, path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaNode(logicalUnnest)
  case class SparkUnnest(logicalUnnest:Unnest, path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkNode(logicalUnnest)

  case class ScalaOuterJoin(logicalOuterJoin:OuterJoin, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode(logicalOuterJoin)
  case class SparkOuterJoin(logicalOuterJoin:OuterJoin, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode(logicalOuterJoin)

  case class ScalaOuterUnnest(logicalOuterUnnest:OuterUnnest, path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends ScalaNode(logicalOuterUnnest)
  case class SparkOuterUnnest(logicalOuterUnnest:OuterUnnest, path: Exp, pred: Exp, child: PhysicalAlgebraNode) extends SparkNode(logicalOuterUnnest)

  case class ScalaMerge(lNode:Merge, m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends ScalaNode(lNode)
  case class SparkMerge(lNode:Merge, m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends SparkNode(lNode)

  case class ScalaToSparkNode(node:ScalaNode) extends SparkNode(node.logicalNode)
}

