package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.util.Environments

case class Symbol(idn: String) extends RawNode

object SymbolTable extends Environments with LazyLogging {

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

  val counterByIdn = scala.collection.mutable.HashMap[Idn, Int]()

  def nextByIdn(idn: Idn): Symbol = {
    val lookupIdn = idn.takeWhile(_ != '$')
    if (!counterByIdn.contains(lookupIdn))
      counterByIdn.put(lookupIdn, 0)
    val n = counterByIdn(lookupIdn)
    counterByIdn.put(lookupIdn, n + 1)
    Symbol(s"$lookupIdn$$$n")
  }

  /** Reset the symbol table.
   */
  def reset() {
    counter.reset()
    counterByIdn.clear()
  }

  sealed abstract class RawEntity extends Entity {
    /** Unique identifier for an entity.
      */
    val id = next()
  }

  /** Entity for a variable (aka. identifier).
    */
  case class VariableEntity(idn: IdnDef, t: Type) extends RawEntity {
    override val id = nextByIdn(idn.idn)
  }

  /** Entity for a data source.
    */
  case class DataSourceEntity(sym: Symbol) extends RawEntity

  /** Entity for auto-detected record attribute over a generator.
    */
  case class GenAttributeEntity(att: AttrType, g: Gen, idx: Int) extends Entity

  /** Entity for auto-detected record attribute over a into.
    */
  case class IntoAttributeEntity(att: AttrType, i: Into, idx: Int) extends Entity

  /** Entity for a partition.
    */
  case class PartitionEntity(s: Select, t: Type) extends Entity

  /** Entity for a *.
    */
  case class StarEntity(s: Select, t: Type) extends Entity

}