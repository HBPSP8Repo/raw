package raw.calculus

import org.kiama.util.Environments
import raw._

object SymbolTable extends Environments {

  import org.kiama.util.{Counter, Entity}
  import Calculus.Exp

  val bindCounter = new Counter(0)
  val genCounter = new Counter(0)
  val funArgCounter = new Counter(0)
  val clsCounter = new Counter(0)

  /** Reset the symbol table.
   */
  def reset() {
    bindCounter.reset()
    genCounter.reset()
    funArgCounter.reset()
    clsCounter.reset()
  }

  /** A bound variable entity.
   */
  case class BindVar(e: Exp) extends Entity {
    val locn = {
      val loc = bindCounter.value
      bindCounter.next()
      loc
    }
    override def toString() = s"var${locn.toString}"
  }

  /** A generator variable entity.
   */
  case class GenVar(e: Exp) extends Entity {
    val locn = {
      val loc = genCounter.value
      genCounter.next()
      loc
    }
    override def toString() = s"gen${locn.toString}"
  }

  /** A function argument entity.
   */
   case class FunArg(t: Type) extends Entity {
    val locn = {
      val loc = funArgCounter.value
      funArgCounter.next()
      loc
    }
    override def toString() = s"arg${locn.toString}"
  }

  /** A class entity.
   */
 case class ClassEntity(name: String, t: Type) extends Entity {
    override def toString() = name
  }

}