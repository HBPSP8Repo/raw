package raw
package calculus

case class UniquifierError(err: String) extends RawException(err)

/** Uniquify names
  */
object Uniquifier {

  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable.{RawEntity, DataSourceEntity}

  def apply(tree: Calculus, world: World): Calculus = {
    val analyzer = new SemanticAnalyzer(tree, world)

    def rawEntity(n: IdnNode): RawEntity = analyzer.entity(n) match {
      case e: RawEntity => e
      case e            => throw UniquifierError(s"Entity $e is not a RawEntity")
    }

    rewriteTree(everywhere(rule[IdnNode]{
      case n @ IdnDef(idn, t) => IdnDef(rawEntity(n).id, t)
      case n @ IdnUse(idn)    => rawEntity(n) match {
        case _: DataSourceEntity => IdnUse(idn)    // For data sources, keep the original identifier use.
        case e                   => IdnUse(e.id)   // Otherwise, replace by the internal, globally unique identifier.
      }
    }))(tree)
  }

}
