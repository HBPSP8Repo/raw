package raw.optimizer.reference

import raw.World
import raw.algebra.{LogicalAlgebra, PhysicalAlgebra}
import raw.optimizer.Optimizer

/** Reference optimizer that does a "direct" translation of a logical plan to a physical plan.
  */
object ReferenceOptimizer extends Optimizer {
  def optimize(root: LogicalAlgebra.AlgebraNode, world: World): PhysicalAlgebra.AlgebraNode = {

    def recurse(node: LogicalAlgebra.AlgebraNode): PhysicalAlgebra.AlgebraNode = node match {
      case LogicalAlgebra.Scan(name)                  => { val source = world.getSource(name); PhysicalAlgebra.Scan(source.tipe, source.location) }
      case LogicalAlgebra.Reduce(m, e, ps, child)     => PhysicalAlgebra.Reduce(m, e, ps, recurse(child))
      case LogicalAlgebra.Nest(m, e, f, ps, g, child) => PhysicalAlgebra.Nest(m, e, f, ps, g, recurse(child))
      case LogicalAlgebra.Select(ps, child)           => if (ps.isEmpty) recurse(child) else PhysicalAlgebra.Select(ps, recurse(child))
      case LogicalAlgebra.Join(ps, left, right)       => PhysicalAlgebra.Join(ps, recurse(left), recurse(right))
      case LogicalAlgebra.Unnest(p, ps, child)        => PhysicalAlgebra.Unnest(p, ps, recurse(child))
      case LogicalAlgebra.OuterJoin(ps, left, right)  => PhysicalAlgebra.OuterJoin(ps, recurse(left), recurse(right))
      case LogicalAlgebra.OuterUnnest(p, ps, child)   => PhysicalAlgebra.OuterUnnest(p, ps, recurse(child))
      case LogicalAlgebra.Merge(m, left, right)       => PhysicalAlgebra.Merge(m, recurse(left), recurse(right))
    }

    recurse(root)
  }
}
