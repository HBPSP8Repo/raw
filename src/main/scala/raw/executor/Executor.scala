package raw
package executor

import algebra.Algebra

abstract class RawExecutorException(message: String) extends RawException(message)

case class RawExecutorRuntimeException(message: String) extends RawExecutorException(message) {
  override def toString() = s"raw execution error: $message"
}

abstract class Executor {
  def execute(root: Algebra.OperatorNode, world: World): Either[QueryError,QueryResult]
}
