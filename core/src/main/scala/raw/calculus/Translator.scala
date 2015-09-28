package raw
package calculus
import scala.collection.immutable.Seq

import org.kiama.rewriting.Strategy

/** Desugar SQL to for comprehension
  */
class Translator extends Transformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  private def translate = reduce(inToComp + selectGroupBy) <* reduce(selectToComp)

  // TODO: Add case classes ToSet, ToBag, ToList and apply them here so that the output Comp still types properly

  private lazy val inToComp = rule[Exp] {
    case s @ InExp(e1, e2) => {
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), e2)), BinaryExp(Eq(), IdnExp(IdnUse(x)), e1))
    }
  }

  private lazy val selectToComp = rule[Exp] {
    case s @ Select(from, distinct, None, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()

      val c = Comp(BagMonoid(), from.map { case Iterator(Some(p), e) => Gen(p, UnaryExp(ToBag(), e)) } ++ whereq, proj)
      if (distinct) UnaryExp(ToSet(), c) else c
  }

  private lazy val selectGroupBy = rule[Exp] {
    case s @ Select(from, distinct, Some(groupby), proj, where, None, None) =>
      logger.debug(s"Applying selectGroupBy to ${CalculusPrettyPrinter(s)}")
      val ns = rewriteInternalIdns(deepclone(s))

      assert(ns.from.nonEmpty)

      val nproj =
        if (ns.from.length == 1)
          IdnExp(IdnUse(ns.from.head.idn.get.idn.idn))
        else
          RecordCons(ns.from.zipWithIndex.map { case (f, idx) => AttrCons(s"_${idx + 1}", IdnExp(IdnUse(f.idn.get.idn.idn)))})

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


  private def rewriteInternalIdns[T <: RawNode](n: T): T = {
    val collectIdnDefs = collect[Seq, Idn] {
      case IdnDef(idn) => idn
    }
    val internalIdns = collectIdnDefs(n)

    val ids = scala.collection.mutable.Map[String, String]()

    def newIdn(idn: Idn) = { if (!ids.contains(idn)) ids.put(idn, SymbolTable.next().idn); ids(idn) }

    rewrite(
      everywhere(rule[IdnNode] {
        case IdnDef(idn) if idn.startsWith("$") && internalIdns.contains(idn) => IdnDef(newIdn(idn))
        case IdnUse(idn) if idn.startsWith("$") && internalIdns.contains(idn) => IdnUse(newIdn(idn))
      }))(n)

  }

}