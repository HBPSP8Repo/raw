package raw
package calculus

import org.kiama.util.Environments
import raw.calculus.Calculus.IdnDef

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

  /** Entity for a variable (aka. identifier).
    */
  case class VariableEntity(idn: IdnDef) extends RawEntity

  /** Entity for a data source.
    */
  case class DataSourceEntity(name: String) extends RawEntity
}