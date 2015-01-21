package raw.executor

import raw.DataLocation
import raw.logical.Algebra.OperatorNode
import raw.calculus.World

/**
 * Created by gaidioz on 1/14/15.
 */

abstract class Executor(schema: World, dataLocations: Map[String, DataLocation]) {
  def execute(operatorNode: OperatorNode): ExecutorResult
}

abstract class ExecutorResult {
  def value: Any
}