package raw
package executor

import algebra.Algebra

// TODO: Remove hierarchy
abstract class RawExecutorException extends RawException

// TODO: Rename to ExecutorError
case class RawExecutorRuntimeException(message: String) extends RawExecutorException {
  // TODO: Fix toString
  override def toString() = s"raw execution error: $message"
}

abstract class Executor {
  def execute(root: Algebra.OperatorNode, world: World): Either[QueryError,QueryResult]
}
