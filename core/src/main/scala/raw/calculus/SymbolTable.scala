package raw
package calculus

import org.kiama.util.Environments

case class Symbol(idn: String) extends RawNode

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}
  import Calculus._

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

  /** Entity for auto-detected record attribute.
    */
  case class AttributeEntity(att: AttrType, g: Gen, idx: Int) extends Entity

  /** Entity for a partition.
    */
  case class PartitionEntity(s: Select, t: Type) extends Entity

  /** Entity for a *.
    */
  case class StarEntity(e: Exp, t: Type) extends Entity

}