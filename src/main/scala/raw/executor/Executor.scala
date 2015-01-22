package raw.executor

import raw.{QueryError, QueryResult, World}
import raw.algebra.PhysicalAlgebra

abstract class Executor {
  def execute(root: PhysicalAlgebra.AlgebraNode, world: World): Either[QueryError,QueryResult]
}
