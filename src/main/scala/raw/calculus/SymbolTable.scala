package raw
package calculus

import org.kiama.util.Environments

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}
  import Calculus.Exp

  val counter = new Counter(0)

  /** Return the next unique identifier.
    */
  def next(): String = {
    val n = counter.value
    counter.next()
    s"$$$n"
  }

  /** Reset the symbol table.
   */
  def reset() {
    counter.reset()
  }

  sealed abstract class RawEntity extends Entity {
    /** Unique identifier for an entity.
      */
    val id = next()
  }

  /** Entity for a bind statement.
    */
  case class BindEntity(t: Option[Type], e: Exp) extends RawEntity

  /** Entity for a generator statement.
    */
  case class GenEntity(t: Option[Type], e: Exp) extends RawEntity

  /** Entity for a function abstraction.
    */
  case class FunAbsEntity(t: Option[Type]) extends RawEntity

  /** Entity for a user-defined class extent.
    */
  case class ClassEntity(name: String, t: Type) extends RawEntity

}