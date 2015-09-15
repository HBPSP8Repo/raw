package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

trait Transformer extends LazyLogging {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  /** Splits a list using a partial function.
    */
  protected def splitWith[A, B](xs: Seq[A], f: PartialFunction[A, B]): Option[(Seq[A], B, Seq[A])] = {
    val begin = xs.takeWhile(x => if (!f.isDefinedAt(x)) true else false)
    if (begin.length == xs.length) {
      None
    } else {
      val elem = f(xs(begin.length))
      if (begin.length + 1 == xs.length) {
        Some((begin, elem, Seq()))
      } else {
        Some(begin, elem, xs.takeRight(xs.length - begin.length - 1))
      }
    }
  }

  /** Rewrite all identifiers in the expression using new global identifiers.
    * Takes care to only rewrite identifiers that are system generated, and not user-defined class extent names.
    */
  protected def rewriteIdns[T <: RawNode](n: T): T = {
    val ids = scala.collection.mutable.Map[String, String]()

    def newIdn(idn: Idn) = { if (!ids.contains(idn)) ids.put(idn, SymbolTable.next().idn); ids(idn) }

    rewrite(
      everywhere(rule[IdnNode] {
        case IdnDef(idn) if idn.startsWith("$") => IdnDef(newIdn(idn))
        case IdnUse(idn) if idn.startsWith("$") => IdnUse(newIdn(idn))
      }))(n)
  }

}