package raw
package calculus

import org.kiama.attribution.Attribution

/** Desugar SQL to for comprehension
  */
trait Translator extends Attribution with Transformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  def analyzer: SemanticAnalyzer

  private def translate = reduce(selectToComp)

  // TODO: Keyword "partition" ? Type it...
  // TODO: Add case classes ToSet, ToBag, ToList and apply them here so that the output Comp still types properly

  private lazy val selectToComp = rule[Exp] {
    case s @ Select(from, _, _, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()
      analyzer.tipe(s) match {
        case CollectionType(m, _) => Comp(m, from.map { case Iterator(Some(p), e) => Gen(p, e)} ++ whereq, proj)
      }
    case s @ Select(from, _, Some(g), proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()
      analyzer.tipe(s) match {
        case CollectionType(m, _) => Comp(m, from.map { case Iterator(Some(p), e) => Gen(p, e)} ++ whereq, proj)
      }
  }

}

object Translator {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree)
    val t2 = Uniquifier(t1, world)
    val a = new SemanticAnalyzer(t2, world)
    val translator = new Translator {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(translator.strategy)(t2)
  }
}
