package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

case class CanonizerError(err: String) extends RawException

/** Canonize a comprehension.
  */
object Canonizer extends LazyLogging {

  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable.{GenVar, ClassEntity}

  /** Transform a normalized comprehension into its canonical form.
    * The canonical form is described in [1], page 19.
    */
  def apply(tree: Calculus, world: World): Calculus = {
    val inTree = Normalizer(tree, world)
    logger.debug(s"Canonizer input tree: ${CalculusPrettyPrinter.pretty(inTree.root)}")

    val analyzer = new SemanticAnalyzer(inTree, world)

    val strategy = everywhere(rule[Exp]{
      case Comp(m, qs, e) => {
        def toPath(e: Exp): Path = e match {
          case IdnExp(idn)       => analyzer.entity(idn) match {
            case _: GenVar            => BoundPath(idn)
            case ClassEntity(name, _) => ClassExtent(name)
          }
          case RecordProj(e, idn) => InnerPath(toPath(e), idn)
          case _                  => throw CanonizerError(s"Unexpected expression in path: $e")
        }

        val paths = qs.collect { case g: Gen => g }.map { case Gen(idn, e) => GenPath(idn, toPath(e)) }
        val preds = qs.collect { case e: Exp => e }
        CanonicalComp(m, paths.toList, preds.toList, e)
      }
    })

    val outTree = rewriteTree(strategy)(inTree)
    logger.debug(s"Canonizer output tree: ${CalculusPrettyPrinter.pretty(outTree.root)}")
    outTree
  }

}