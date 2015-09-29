package raw
package calculus

/** Desugar the expressions such as Count, Max, ...
  */
class ExpressionsDesugarer(val analyzer: SemanticAnalyzer) extends Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = desugar

  private lazy val desugar =
    reduce(
      ruleSum +
      ruleMax +
      ruleCount +
      ruleIn +
      ruleExists +
      selectGroupBy)

  /** De-sugar sum
    */
  private lazy val ruleSum = rule[Exp] {
    case Sum(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), idnExp)), IdnExp(IdnUse(x)))), UnaryExp(ToBag(), e))
        case _: CollectionType               => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), idnExp)), IdnExp(IdnUse(x)))), e)
      }
  }

  /** De-sugar max
    */
  private lazy val ruleMax = rule[Exp] {
    case Max(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(MaxMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), IdnExp(IdnUse(xs)))), IdnExp(IdnUse(x)))), e)
  }

  /** De-sugar count
    */
  private lazy val ruleCount = rule[Exp] {
    case Count(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), idnExp)), IntConst("1"))), UnaryExp(ToBag(), e))
        case _: CollectionType               => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), idnExp)), IntConst("1"))), e)
      }
  }

  /** De-sugar in
    */
  private lazy val ruleIn = rule[Exp] {
    case s @ InExp(e1, e2) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), e2)), BinaryExp(Eq(), IdnExp(IdnUse(x)), e1))
  }

  /** De-sugar exists
    */
  private lazy val ruleExists = rule[Exp] {
    case Exists(e) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(PatternIdn(IdnDef(x)), e)), BoolConst(true))
  }

  /** De-sugar a SELECT with a GROUP BY
    */
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