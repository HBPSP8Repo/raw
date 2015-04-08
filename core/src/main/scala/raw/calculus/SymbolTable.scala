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
  case class BindEntity(t: Type, e: Exp) extends RawEntity

  /** Entity for a generator statement.
    */
  case class GenEntity(t: Type, e: Exp) extends RawEntity

  /** Entity for the argument of function abstraction.
    */
  case class FunArgEntity(t: Type) extends RawEntity

  /** Entity for a pattern bind statement.
    */
  case class PatternBindEntity(t: Type, e: Exp, idxs: Seq[Int]) extends RawEntity

  /** Entity for a pattern generator statement.
    */
  case class PatternGenEntity(t: Type, e: Exp, idxs: Seq[Int]) extends RawEntity

  /** Entity for a data source.
    */
  case class DataSourceEntity(name: String) extends RawEntity

}