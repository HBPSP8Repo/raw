package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

/** Desugar SQL to for comprehension
  */
trait Translator extends Transformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  def analyzer: SemanticAnalyzer

  private def translate = reduce(selectGroupBy) <* reduce(selectToComp)

  // TODO: Add case classes ToSet, ToBag, ToList and apply them here so that the output Comp still types properly

  private lazy val selectToComp = rule[Exp] {
    case s @ Select(from, _, None, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()

      // TODO: Handlie distinct!!
      // TODO: Turn it allinto ToBag always and have the Bag Monoid unless it is distinct
      Comp(BagMonoid(), from.map { case Iterator(Some(p), e) => Gen(p, e) } ++ whereq, proj)
  }

  private lazy val selectGroupBy = rule[Exp] {
    case s @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Applying selectGroupBy")
      val ns = rewriteIdns(deepclone(s))

      assert(ns.from.nonEmpty)

      val nproj =
        if (ns.from.length == 1)
          IdnExp(IdnUse(ns.from.head.idn.get.idn.idn))
        else
          RecordCons(ns.from.zipWithIndex.map { case (f, idx) => AttrCons(s"_${idx + 1}", IdnExp(IdnUse(f.idn.get.idn.idn)))})

      val partition =
        if (ns.where.isDefined)
          Select(ns.from, ns.distinct, None, nproj, Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(groupby), ns.group.get))), None, None)
        else
          Select(ns.from, ns.distinct, None, nproj, Some(BinaryExp(Eq(), deepclone(groupby), ns.group.get)), None, None)

      val projWithoutPart = rewrite(everywherebu(rule[Exp] {
        case p: Partition =>
          deepclone(partition)
      }))(proj)

      Select(from, distinct, None, projWithoutPart, where, None, None)
  }
}

object Translator extends LazyLogging {

  import org.kiama.rewriting.Rewriter.rewriteTree
  import Calculus.Calculus

  def apply(tree: Calculus, world: World): Calculus = {
    val t1 = Desugarer(tree, world)
    val a = new SemanticAnalyzer(t1, world)
    val translator = new Translator {
      override def analyzer: SemanticAnalyzer = a
    }
    rewriteTree(translator.strategy)(t1)
  }
}
