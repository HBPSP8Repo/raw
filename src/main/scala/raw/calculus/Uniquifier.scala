package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify variable names.
  */
object Uniquifier extends LazyLogging {

  import Calculus.{Calculus, Idn, IdnNode, IdnDef, IdnUse}
  import SymbolTable.{ClassEntity, RawEntity}
  import org.kiama.rewriting.Rewriter._

  /** Transform all identifiers in the AST into identifiers that are unique.
    */
  def apply(tree: Calculus, world: World): Calculus = {
    logger.debug(s"Uniquifier input tree: ${CalculusPrettyPrinter(tree.root)}")

    val analyzer = new SemanticAnalyzer(tree, world)

    def entity(n: IdnNode): RawEntity = analyzer.entity(n) match {
      case e: RawEntity => e
      case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
    }

    val renameIdns = everywhere(rule[IdnNode]{
      case n @ IdnDef(idn, t) => IdnDef(entity(n).id, t)
      case n @ IdnUse(idn) => entity(n) match {
        case _: ClassEntity => IdnUse(idn)    // For class entities, keep the original identifier use.
        case e              => IdnUse(e.id)   // Otherwise, replace by the internal, globally unique identifier.
      }
    })

    val outTree = rewriteTree(renameIdns)(tree)
    logger.debug(s"Uniquifier output tree: ${CalculusPrettyPrinter(outTree.root)}")
    outTree
  }

}