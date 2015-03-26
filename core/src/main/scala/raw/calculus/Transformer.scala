package raw.calculus

trait Transformer extends SemanticAnalyzer {

  import org.kiama.rewriting.Strategy
  import org.kiama.rewriting.Rewriter.rewriteTree

  def strategy: Strategy

  def transform = rewriteTree(strategy)(tree)

}
