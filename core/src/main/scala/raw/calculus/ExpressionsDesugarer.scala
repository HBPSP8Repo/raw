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
      ruleCount)

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

}