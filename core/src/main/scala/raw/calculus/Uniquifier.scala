package raw
package calculus

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify names
  */
class Uniquifier(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable._

  def transform = uniquify

  private def rawEntity(n: IdnNode): RawEntity = analyzer.entity(n) match {
    case e: RawEntity => e
    case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
  }

  private lazy val uniquify = everywhere(rule[IdnNode] {
    case n @ IdnDef(idn) => rawEntity(n) match {
      case v: VariableEntity => IdnDef(v.id.idn)
    }
    case n @ IdnUse(idn) => rawEntity(n) match {
      case _: DataSourceEntity => IdnUse(idn)       // For data sources, keep the original identifier use.
      case v: VariableEntity   => IdnUse(v.id.idn)  // Otherwise, replace by the newly-generated unique identifier.
    }
  })
}