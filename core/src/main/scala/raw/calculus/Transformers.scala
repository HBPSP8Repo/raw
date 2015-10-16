package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.Seq

/** A transformer.
  * Rewrites the tree by applying a strategy.
  */
trait Transformer extends LazyLogging {

  import org.kiama.rewriting.Strategy
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def strategy: Strategy

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
    * Takes care to only rewrite identifiers that are:
    * - system generated (i.e. excluding extent names)
    * - defined within the sub-tree.
    */
  protected def rewriteInternalIdns[T <: RawNode](n: T): T = {
    val collectIdnDefs = collect[Seq, Idn] {
      case IdnDef(idn) =>
        assert(idn.contains("$"))
        idn
    }
    val idns = collectIdnDefs(n)

    val ids = scala.collection.mutable.Map[String, String]()

    def newIdn(idn: Idn) = { if (!ids.contains(idn)) ids.put(idn, SymbolTable.next().idn); ids(idn) }

    rewrite(
      everywhere(rule[IdnNode] {
        case IdnDef(idn) if idns.contains(idn) =>
          logger.debug(s"1here with $idn and ${newIdn(idn)}")
          IdnDef(newIdn(idn))
        case IdnUse(idn) if idns.contains(idn) =>
          logger.debug(s"2here with $idn and ${newIdn(idn)}")
          IdnUse(newIdn(idn))
      }))(n)
  }
}

/** A transformer that requires the analyzer.
  */
trait SemanticTransformer extends Transformer {
  def analyzer: SemanticAnalyzer
}

/** A transformer that can be pipelined with other transformers.
  */
trait PipelinedTransformer extends Transformer

