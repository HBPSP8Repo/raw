package raw
package calculus

import org.kiama.attribution.Attribution

/** Desugar the expressions such as Count, Max, ...
  */
class ExpressionsDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

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
      ruleMultiCons)

  /** De-sugar sum
    */
  private lazy val ruleSum = rule[Exp] {
    case Sum(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), idnExp)), IdnExp(IdnUse(x)))), UnaryExp(ToBag(), e))
        case _: CollectionType               => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), idnExp)), IdnExp(IdnUse(x)))), e)
      }
  }

  /** De-sugar max
    */
  private lazy val ruleMax = rule[Exp] {
    case Max(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(MaxMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), IdnExp(IdnUse(xs)))), IdnExp(IdnUse(x)))), e)
  }

  /** De-sugar count
    */
  private lazy val ruleCount = rule[Exp] {
    case Count(e) =>
      val xs = SymbolTable.next().idn
      val x = SymbolTable.next().idn
      val idnExp = IdnExp(IdnUse(xs))
      analyzer.tipe(e) match {
        case CollectionType(_: SetMonoid, _) => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), idnExp)), IntConst("1"))), UnaryExp(ToBag(), e))
        case _: CollectionType               => FunApp(FunAbs(PatternIdn(IdnDef(xs)), Comp(SumMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), idnExp)), IntConst("1"))), e)
      }
  }

  /** De-sugar in
    */
  private lazy val ruleIn = rule[Exp] {
    case s @ InExp(e1, e2) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), e2)), BinaryExp(Eq(), IdnExp(IdnUse(x)), e1))
  }

  /** De-sugar exists
    */
  private lazy val ruleExists = rule[Exp] {
    case Exists(e) =>
      val x = SymbolTable.next().idn
      Comp(OrMonoid(), Seq(Gen(Some(PatternIdn(IdnDef(x))), e)), BoolConst(true))
  }

  /** De-sugar MultiCons
    */
  private lazy val ruleMultiCons = rule[Exp] {
    case MultiCons(m, Nil) => ZeroCollectionMonoid(m)
    case MultiCons(m, head :: Nil) => ConsCollectionMonoid(m, head)
    case MultiCons(m, head :: tail) => MergeMonoid(m, ConsCollectionMonoid(m, head), MultiCons(m, tail))
  }

}