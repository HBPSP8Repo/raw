package raw.optimizer

import raw.World
import raw.algebra.{LogicalAlgebra, PhysicalAlgebra}

abstract class Optimizer {
  def optimize(root: LogicalAlgebra.AlgebraNode, world: World): PhysicalAlgebra.AlgebraNode
}