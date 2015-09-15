package raw
package calculus

import org.kiama.rewriting.Rewriter._
import raw.calculus.Calculus.{IdnUse, IdnDef, IdnNode}
import raw.calculus.SymbolTable.{DataSourceEntity, RawEntity}

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify names
  */
trait Uniquifier extends Transformer {

  def analyzer: SemanticAnalyzer

  def strategy = uniquify

  private def rawEntity(n: IdnNode): RawEntity = analyzer.entity(n) match {
    case e: RawEntity => e
    case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
  }

  private lazy val uniquify = everywhere(rule[IdnNode] {
    case n @ IdnDef(idn) => IdnDef(rawEntity(n).id.idn)
    case n @ IdnUse(idn) => rawEntity(n) match {
      case _: DataSourceEntity => IdnUse(idn) // For data sources, keep the original identifier use.
      case e                   => IdnUse(e.id.idn) // Otherwise, replace by the internal, globally unique identifier.
    }
  })
}

object Uniquifier {

  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def apply(tree: Calculus, world: World): Calculus = {
    val a = new SemanticAnalyzer(tree, world)
    val uniquifier = new Uniquifier {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(uniquifier.strategy)(tree)
  }

}
