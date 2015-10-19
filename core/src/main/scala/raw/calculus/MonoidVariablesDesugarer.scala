package raw
package calculus

class MonoidVariablesDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import org.kiama.rewriting.Rewriter._

  def strategy = desugar

  private lazy val desugar =
    reduce(desugarVars)

  def looksLikeSet(m: MonoidVariable) = {
    val p = analyzer.monoidProps(m)
    p.commutative.isDefined && p.commutative.get && p.idempotent.isDefined && p.idempotent.get
  }

  def looksLikeList(m: MonoidVariable) = {
    val p = analyzer.monoidProps(m)
    p.commutative.isDefined && !p.commutative.get && p.idempotent.isDefined && !p.idempotent.get
  }

  private lazy val desugarVars = rule[RawNode] {
    case m: MonoidVariable => analyzer.looksLikeMonoid(m)
  }

}
