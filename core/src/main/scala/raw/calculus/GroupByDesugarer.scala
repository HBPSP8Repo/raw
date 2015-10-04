package raw
package calculus

class GroupByDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable._

  def strategy = desugar

  private lazy val desugar =
    //attempt(manybu(desugarPartition)) <* reduce(dropGroupBy)
    reduce(selectGroupBy)

  private lazy val desugarPartition = rule[Exp] {
    case p: Partition =>
      analyzer.partitionEntity(p) match {
        case PartitionEntity(s, t) =>
          val ns = rewriteInternalIdns(deepclone(s))

          if (ns.where.isDefined)
            Select(ns.from, ns.distinct, None, Star(), Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(s.group.get), ns.group.get))), None, None)
          else
            Select(ns.from, ns.distinct, None, Star(), Some(BinaryExp(Eq(), deepclone(s.group.get), ns.group.get)), None, None)
      }
  }

  private lazy val dropGroupBy = rule[Exp] {
    case Select(from, distinct, Some(_), proj, where, order, having) =>
      Select(from, distinct, None, proj, where, order, having)
  }

  /** De-sugar a SELECT with a GROUP BY
    */


  // TODO: Rewrite this to use the partition entity notion.
  // TODO: Could be split into 2 rules: one to replace partition by a corresponding Select,
  // TODO: and another to rewrite the Select itself to remove the groupby part.
  // TODO: Although removing the groupby part isn't exactly mission critical.
  // TODO: If done bottom up, I don't see an issue, as long as the two rules are done together.
  private lazy val selectGroupBy = rule[Exp] {
    case s @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Applying selectGroupBy to ${CalculusPrettyPrinter(s)}")
      val ns = rewriteInternalIdns(deepclone(s))

      assert(ns.from.nonEmpty)

      val nproj =
        if (ns.from.length == 1)
          IdnExp(IdnUse(ns.from.head.p.get.asInstanceOf[PatternIdn].idn.idn))
        else
          // TODO: THis is now INCORRECT ALCTUALLY
          RecordCons(ns.from.zipWithIndex.map { case (f, idx) => AttrCons(s"_${idx + 1}", IdnExp(IdnUse(f.p.get.asInstanceOf[PatternIdn].idn.idn)))})

      val partition =
        if (ns.where.isDefined)
          Select(ns.from, false, None, nproj, Some(MergeMonoid(AndMonoid(), ns.where.get, BinaryExp(Eq(), deepclone(groupby), ns.group.get))), None, None)
        else
          Select(ns.from, false, None, nproj, Some(BinaryExp(Eq(), deepclone(groupby), ns.group.get)), None, None)

      val projWithoutPart = rewrite(everywherebu(rule[Exp] {
        case p: Partition =>
          deepclone(partition)
      }))(proj)

      val os = Select(from, distinct, None, projWithoutPart, where, None, None)
      logger.debug(s"Output is ${CalculusPrettyPrinter(os)}")
      os
  }
}
