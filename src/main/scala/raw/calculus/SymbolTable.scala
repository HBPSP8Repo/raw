package raw.calculus

import org.kiama.util.Environments

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}
  import raw.Variable

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

  /** Entity for a variable/identifier.
    */
  case class VariableEntity(v: Variable) extends RawEntity

  /** Entity for a user-defined class extent.
    */
  case class ClassEntity(name: String, v: Variable) extends RawEntity

}