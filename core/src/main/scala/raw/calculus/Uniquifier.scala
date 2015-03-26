package raw
package calculus

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify variable names by transform all identifiers in the AST into unique auto-generated identifiers.
  */
trait Uniquifier extends Transformer {

  import Calculus.{IdnNode, IdnDef, IdnUse}
  import SymbolTable.{DataSourceEntity, RawEntity}
  import org.kiama.rewriting.Rewriter._

  def strategy = uniquify

  private lazy val uniquify = everywhere(rule[IdnNode]{
    case n @ IdnDef(idn, t) => IdnDef(rawEntity(n).id, t)
    case n @ IdnUse(idn)    => rawEntity(n) match {
      case _: DataSourceEntity => n              // For data sources, keep the original identifier use.
      case e                   => IdnUse(e.id)   // Otherwise, replace by the internal, globally unique identifier.
    }
  })

  private def rawEntity(n: IdnNode): RawEntity = entity(n) match {
    case e: RawEntity => e
    case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
  }

}