package raw.psysicalalgebra

import raw.algebra.LogicalAlgebra
import raw.algebra.LogicalAlgebra._
import raw.psysicalalgebra.PhysicalAlgebra._

object LogicalToPhysicalAlgebra {
  def apply(root: LogicalAlgebraNode, isSpark: Map[String, Boolean]): PhysicalAlgebraNode = {
    buildPhysicalTree(root, isSpark)
  }

  private def buildPhysicalTree(root: LogicalAlgebraNode, isSpark: Map[String, Boolean]): PhysicalAlgebraNode = {
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

    def recurse(l: LogicalAlgebraNode): PhysicalAlgebraNode = l match {
      case LogicalAlgebra.Scan(name, t) =>
        if (isSpark(name)) SparkScan(name, t) else ScalaScan(name, t)

      case LogicalAlgebra.Select(p, child) =>
        helper1(child, c => ScalaSelect(p, c), c => SparkSelect(p, c))
      case Reduce(m, e, p, child) =>
        helper1(child, c => ScalaReduce(m, e, p, c), c => SparkReduce(m, e, p, c))
      case Nest(m, e, f, p, g, child) =>
        helper1(child, c => ScalaNest(m, e, f, p, g, c), c => SparkNest(m, e, f, p, g, c))
      case OuterUnnest(path, pred, child) =>
        helper1(child, c => ScalaOuterUnnest(path, pred, c), c => SparkOuterUnnest(path, pred, c))
      case Unnest(path, pred, child) =>
        helper1(child, c => ScalaUnnest(path, pred, c), c => SparkUnnest(path, pred, c))

      // Rules which may require promotion of Java to Spark
      case Merge(m, left, right) =>
        helper2(left, right, (l, r) => ScalaMerge(m, l, r), (l, r) => SparkMerge(m, l, r))
      case Join(p, left, right) =>
        helper2(left, right, (l, r) => ScalaJoin(p, l, r), (l, r) => SparkJoin(p, l, r))
      case OuterJoin(p, left, right) =>
        helper2(left, right, (l, r) => ScalaOuterJoin(p, l, r), (l, r) => SparkOuterJoin(p, l, r))
    }

    recurse(root)
  }
}
