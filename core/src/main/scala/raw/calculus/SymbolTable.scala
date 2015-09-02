package raw
package calculus

import org.kiama.util.Environments
import raw.calculus.Calculus.{Idn, IdnDef}

case class Symbol(idn: String)

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}

  val counter = new Counter(0)

  /** Return the next unique identifier.
    */
  def next(): Symbol = {
    val n = counter.value
    counter.next()
    Symbol(s"$$$n")
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
  case class VariableEntity(idn: IdnDef, t: Type) extends RawEntity

  /** Entity for a data source.
    */
  case class DataSourceEntity(sym: Symbol) extends RawEntity
}