package raw
package calculus

import org.kiama.rewriting.Rewriter._
import raw.calculus.Calculus.{IdnUse, IdnDef, IdnNode}
import raw.calculus.SymbolTable.{DataSourceEntity, RawEntity}

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify names
  */
class Uniquifier(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  def strategy = uniquify

  private def rawEntity(n: IdnNode): RawEntity = analyzer.entity(n) match {
    case e: RawEntity => e
    case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
  }

  private lazy val uniquify = everywhere(rule[IdnNode] {
    case n @ IdnDef(idn) => IdnDef(rawEntity(n).id.idn)
    case n @ IdnUse(idn) => rawEntity(n) match {
      case _: DataSourceEntity => IdnUse(idn)      // For data sources, keep the original identifier use.
      case e                   => IdnUse(e.id.idn) // Otherwise, replace by the internal, globally unique identifier.
    }
  })
}