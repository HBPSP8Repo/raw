package raw
package calculus

/** Desugar a comprehension.
  */
trait Desugarer extends Transformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  def strategy = desugar

  private lazy val desugar =
    reduce(rulePatternFunAbs +
      rulePatternGen +
      rulePatternBindExpBlock +
      rulePatternBindComp +
      ruleExpBlocks +
      ruleEmptyExpBlock)

  /** De-sugar pattern function abstractions into expression blocks.
    * e.g. `\((a,b),c) -> a + b + c` becomes `\x -> { (a,b) := x._1; c := x._2; a + b + c }`
    */

  private lazy val rulePatternFunAbs = rule[Exp] {
    case FunAbs(PatternProd(ps), e) =>
      logger.debug("Applying desugar rulePatternFunAbs")
      val idn = SymbolTable.next().idn
      FunAbs(
        PatternIdn(IdnDef(idn)),
        ExpBlock(ps.zipWithIndex.map { case (p, idx) => Bind(p, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}")) }, e))
  }

  /** De-sugar pattern generators.
    * e.g. `for ( ((a,b),c) <- X; ...)` becomes `for (x <- X; ((a,b),c) := x; ...)`
    */

  private object RulePatternGen {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Gen](qs, { case g @ Gen(_: PatternProd, _) => g })
  }

  private lazy val rulePatternGen = rule[Comp] {
    case Comp(m, RulePatternGen(r, Gen(p, u), s), e) =>
      logger.debug("Applying desugar rulePatternGen")
      val idn = SymbolTable.next().idn
      Comp(m, r ++ Seq(Gen(PatternIdn(IdnDef(idn)), u), Bind(p, IdnExp(IdnUse(idn)))) ++ s, e)
  }

  /** De-sugar pattern binds inside expression blocks.
    * e.g. `{ ((a,b),c) = X; ... }` becomes `{ (a,b) = X._1; c = X._2; ... }`
    */

  private lazy val rulePatternBindExpBlock = rule[ExpBlock] {
    case ExpBlock(Bind(PatternProd(ps), u) :: rest, e) =>
      logger.debug("Applying desugar rulePatternBindExpBlock")
      ExpBlock(ps.zipWithIndex.map { case (p, idx) => Bind(p, RecordProj(deepclone(u), s"_${idx + 1}")) } ++ rest, e)
  }

  /** De-sugar pattern binds inside comprehension qualifiers.
    * e.g. `for (...; ((a,b),c) = X; ...)` becomes `for (...; (a,b) = X._1; c = X._2; ...)`
    */

  private object RulePatternBindComp {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, Bind](qs, { case b @ Bind(_: PatternProd, _) => b })
  }

  private lazy val rulePatternBindComp = rule[Comp] {
    case Comp(m, RulePatternBindComp(r, Bind(PatternProd(ps), u), s), e) =>
      logger.debug("Applying desugar rulePatternBindComp")
      Comp(m, r ++ ps.zipWithIndex.map { case (p, idx) => Bind(p, RecordProj(deepclone(u), s"_${idx + 1}")) } ++ s, e)
  }

  /** De-sugar expression blocks by removing the binds one-at-a-time.
    */

  private lazy val ruleExpBlocks = rule[ExpBlock] {
    case in @ ExpBlock(Bind(PatternIdn(IdnDef(x)), u) :: rest, e) =>
      logger.debug(s"Applying desugar ruleExpBlocks to ${CalculusPrettyPrinter(in, 200)}")
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => deepclone(u)
      })
      val nrest = rewrite(strategy)(rest)
      val ne = rewrite(strategy)(e)
      val out = ExpBlock(nrest, ne)
      logger.debug(s"Out is ${CalculusPrettyPrinter(out, 200)}")
      out
  }

  /** De-sugar expression blocks without bind statements.
    */

  private lazy val ruleEmptyExpBlock = rule[Exp] {
    case ExpBlock(Nil, e) =>
      logger.debug("Applying desugar ruleEmptyExpBlock")
      e
  }

}

object Desugarer {

  import Calculus.Calculus
  import org.kiama.rewriting.Rewriter.rewriteTree

  def apply(tree: Calculus): Calculus = {
    val desugarer = new Desugarer {}
    rewriteTree(desugarer.strategy)(tree)
  }

}