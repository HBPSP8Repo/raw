package raw.psysicalalgebra

import raw.algebra.LogicalAlgebra
import raw.algebra.LogicalAlgebra._
import raw.psysicalalgebra.PhysicalAlgebra._

import scala.collection.mutable

object LogicalToPhysicalAlgebra {
  def apply(root: LogicalAlgebraNode, isSpark: Map[String, Boolean]): PhysicalAlgebraNode = {
    buildPhysicalTree(root, isSpark)
  }

  private def buildPhysicalTree(root: LogicalAlgebraNode, isSpark: Map[String, Boolean]): PhysicalAlgebraNode = {
    // Make a mutable copy, so we can store the type of the assign nodes.
    val isSparkAux: mutable.Map[String, Boolean] = mutable.Map(isSpark.toSeq: _*)

    def convertAssign(key: String, node: LogicalAlgebraNode): PhysicalAlgebraNode = {
      val phyNode: PhysicalAlgebraNode = recurse(node)
      // Update the cache with the type of this assign node
      val isSparkNode = phyNode.isInstanceOf[SparkNode]
      isSparkAux.put(key, isSparkNode)
      phyNode
    }

    def helper1(node: LogicalAlgebraNode,
                scalaBuilder: ScalaNode => ScalaNode,
                sparkBuilder: SparkNode => SparkNode): PhysicalAlgebraNode = recurse(node) match {
      case c: ScalaNode => scalaBuilder(c)
      case c: SparkNode => sparkBuilder(c)
    }

    def helper2(left: LogicalAlgebraNode, right: LogicalAlgebraNode,
                scalaBuilder: (ScalaNode, ScalaNode) => ScalaNode,
                sparkBuilder: (SparkNode, SparkNode) => SparkNode): PhysicalAlgebraNode = (recurse(left), recurse(right)) match {
      case (l: SparkNode, r: SparkNode) => sparkBuilder(l, r)
      case (l: SparkNode, r: ScalaNode) => sparkBuilder(l, ScalaToSparkNode(r))
      case (l: ScalaNode, r: SparkNode) => sparkBuilder(ScalaToSparkNode(l), r)
      case (l: ScalaNode, r: ScalaNode) => scalaBuilder(l, r)
    }

    def recurse(logicalNode: LogicalAlgebraNode): PhysicalAlgebraNode = logicalNode match {
      case lNode@LogicalAlgebra.Scan(name, t) =>
        if (isSparkAux.apply(name)) SparkScan(lNode, name, t) else ScalaScan(lNode, name, t)

      case lNode@LogicalAlgebra.Select(p, child) =>
        helper1(child, c => ScalaSelect(lNode, p, c), c => SparkSelect(lNode, p, c))
      case lNode@Reduce(m, e, p, child) =>
        helper1(child, c => ScalaReduce(lNode, m, e, p, c), c => SparkReduce(lNode, m, e, p, c))
      case lNode@Nest(m, e, f, p, g, child) =>
        helper1(child, c => ScalaNest(lNode, m, e, f, p, g, c), c => SparkNest(lNode, m, e, f, p, g, c))
      case lNode@OuterUnnest(path, pred, child) =>
        helper1(child, c => ScalaOuterUnnest(lNode, path, pred, c), c => SparkOuterUnnest(lNode, path, pred, c))
      case lNode@Unnest(path, pred, child) =>
        helper1(child, c => ScalaUnnest(lNode, path, pred, c), c => SparkUnnest(lNode, path, pred, c))
      case lNode@Assign(as, child) =>
        val asPhysicalNodes: Seq[(String, PhysicalAlgebraNode)] = as.map({ case (key, node) => (key, convertAssign(key, node)) })
        helper1(child, c => ScalaAssign(lNode, asPhysicalNodes, c), c => SparkAssign(lNode, asPhysicalNodes, c))

      // Rules which may require promotion of Java to Spark
      case lNode@Merge(m, left, right) =>
        helper2(left, right, (l, r) => ScalaMerge(lNode, m, l, r), (l, r) => SparkMerge(lNode, m, l, r))
      case lNode@Join(p, left, right) =>
        helper2(left, right, (l, r) => ScalaJoin(lNode, p, l, r), (l, r) => SparkJoin(lNode, p, l, r))
      case lNode@OuterJoin(p, left, right) =>
        helper2(left, right, (l, r) => ScalaOuterJoin(lNode, p, l, r), (l, r) => SparkOuterJoin(lNode, p, l, r))
    }

    recurse(root)
  }
}
