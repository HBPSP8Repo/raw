package raw.calculus

import org.kiama.util.Environments

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}
  import raw.Type
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

  /** A bound variable entity.
   */
  case class BindVar(var t: Type) extends RawEntity

  /** A generator variable entity.
   */
  case class GenVar(var t: Type) extends RawEntity

  /** A function argument entity.
   */
   case class FunArg(var t: Type) extends RawEntity

  /** A class entity.
   */
  case class ClassEntity(name: String, var t: Type) extends RawEntity

}