package raw
package calculus

import raw._

/** Desugar the expressions such as Count, Max, ...
  */
class ExpressionsDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import Calculus._
  import org.kiama.rewriting.Cloner._

  import scala.collection.immutable.Seq

  def transform = desugar

  private lazy val desugar =
    manytd(
      ruleSum +
      ruleMax +
      ruleMin +
      ruleCount +
      ruleIn +
      ruleExists +
      ruleMultiCons)

  /** De-sugar sum
    */
  private lazy val ruleSum = rule[Exp] {
    case Sum(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), idnExp)), IdnExp(IdnUse(x)))), Seq(UnaryExp(ToBag(), e)))
        case _: CollectionType               => FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), idnExp)), IdnExp(IdnUse(x)))), Seq(e))
      }
  }

  /** De-sugar max
    */
  private lazy val ruleMax = rule[Exp] {
    case Max(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(MaxMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), IdnExp(IdnUse(xs)))), IdnExp(IdnUse(x)))), Seq(e))
  }

  /** De-sugar min
    */
  private lazy val ruleMin = rule[Exp] {
    case Min(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(MinMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), IdnExp(IdnUse(xs)))), IdnExp(IdnUse(x)))), Seq(e))
  }

  /** De-sugar count
    */
  private lazy val ruleCount = rule[Exp] {
    case Count(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      logger.debug(s"we are at $e and type is ${analyzer.tipe(e)}")
      analyzer.printTypedTree()
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), idnExp)), IntConst("1"))), Seq(UnaryExp(ToBag(), e)))
        case _: CollectionType               => FunApp(FunAbs(Seq(IdnDef(xs, None)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), idnExp)), IntConst("1"))), Seq(e))
      }
  }

  /** De-sugar in
    */
  private lazy val ruleIn = rule[Exp] {
    case s @ InExp(e1, e2) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), e2)), BinaryExp(Eq(), IdnExp(IdnUse(x)), e1))
  }

  /** De-sugar exists
    */
  private lazy val ruleExists = rule[Exp] {
    case Exists(e) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x, None))), e)), BoolConst(true))
  }

  /** De-sugar MultiCons
    */
  private lazy val ruleMultiCons = rule[Exp] {
    case MultiCons(m, Nil) => ZeroCollectionMonoid(m)
    case MultiCons(m, head :: Nil) => ConsCollectionMonoid(m, head)
    case MultiCons(m, head :: tail) =>
      m match {
        case _: SetMonoid => BinaryExp(Union(), ConsCollectionMonoid(m, head), MultiCons(m, tail))
        case _: BagMonoid => BinaryExp(BagUnion(), ConsCollectionMonoid(m, head), MultiCons(m, tail))
        case _: ListMonoid => BinaryExp(Append(), ConsCollectionMonoid(m, head), MultiCons(m, tail))
      }
  }

}