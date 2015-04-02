package raw
package calculus

/** Desugar a comprehension.
  */
object Desugarer extends Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def apply(tree: Calculus): Calculus =
    rewriteTree(desugar)(tree)

  lazy val desugar = reduce(rulePatternFunAbs + rulePatternGen + rulePatternBindExpBlock + rulePatternBindComp)

  /** De-sugar pattern function abstractions into expression blocks.
    * e.g. `\((a,b),c) -> a + b + c` becomes `\x -> { (a,b) := x._1; c := x._2; a + b + c }`
    */

  lazy val rulePatternFunAbs = rule[Exp] {
    case PatternFunAbs(p@PatternProd(ps), e) =>
      logger.debug("Applying desugar rulePatternFunAbs")
      val idn = SymbolTable.next()
      FunAbs(IdnDef(idn, None),
        ExpBlock(
          ps.zipWithIndex.map {
            case (p1, idx) => p1 match {
              case PatternIdn(idn1) => Bind(idn1, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
              case p2: PatternProd  => PatternBind(p2, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
            }
          }, e))
  }

  /** De-sugar pattern generators.
    * e.g. `for ( ((a,b),c) <- X; ...)` becomes `for (x <- X; ((a,b),c) := x; ...)`
    */

  object RulePatternGen {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternGen](qs, { case g: PatternGen => g })
  }

  lazy val rulePatternGen = rule[Comp] {
    case Comp(m, RulePatternGen(r, PatternGen(p, u), s), e) =>
      logger.debug("Applying desugar rulePatternGen")
      val idn = SymbolTable.next()
      Comp(m, r ++ Seq(Gen(IdnDef(idn, None), u), PatternBind(p, IdnExp(IdnUse(idn)))) ++ s, e)
  }

  /** De-sugar pattern binds inside expression blocks.
    * e.g. `{ ((a,b),c) = X; ... }` becomes `{ (a,b) = X._1; c = X._2; ... }`
    */

  lazy val rulePatternBindExpBlock = rule[ExpBlock] {
    case ExpBlock(PatternBind(PatternProd(ps), u) :: rest, e) =>
      logger.debug("Applying desugar rulePatternBindExpBlock")
      ExpBlock(ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(deepclone(u), s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(deepclone(u), s"_${idx + 1}"))
        }
      } ++ rest, e)
  }

  /** De-sugar pattern binds inside comprehension qualifiers.
    * e.g. `for (...; ((a,b),c) = X; ...)` becomes `for (...; (a,b) = X._1; c = X._2; ...)`
    */

  object RulePatternBindComp {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternBind](qs, { case b: PatternBind => b })
  }

  lazy val rulePatternBindComp = rule[Comp] {
    case Comp(m, RulePatternBindComp(r, PatternBind(PatternProd(ps), u), s), e) =>
      logger.debug("Applying desugar rulePatternBindComp")
      Comp(m, r ++ ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(deepclone(u), s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(deepclone(u), s"_${idx + 1}"))
        }
      } ++ s, e)
  }

}
