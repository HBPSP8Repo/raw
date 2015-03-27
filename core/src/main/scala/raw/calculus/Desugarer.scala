package raw
package calculus

/** Desugar a comprehension.
  */
trait Desugarer extends Uniquifier {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import Calculus._

  override def strategy = super.strategy <* desugar

  lazy val desugar = reduce(rulePatternFunAbs + rulePatternGen + rulePatternBindExpBlock + rulePatternBindComp +
    ruleExpBlocks + ruleEmptyExpBlock)

  /** Splits a list using a partial function.
    */
  protected def splitWith[A, B](xs: Seq[A], f: PartialFunction[A, B]): Option[Tuple3[Seq[A], B, Seq[A]]] = {
    val begin = xs.takeWhile(x => if (!f.isDefinedAt(x)) true else false)
    if (begin.length == xs.length) {
      None
    } else {
      val elem = f(xs(begin.length))
      if (begin.length + 1 == xs.length) {
        Some((begin, elem, Seq()))
      } else {
        Some(begin, elem, xs.takeRight(xs.length - begin.length - 1))
      }
    }
  }

  /** De-sugar pattern function abstractions into expression blocks.
    * e.g. `\((a,b),c) -> a + b + c` becomes `\x -> { (a,b) := x._1; c := x._2; a + b + c }`
    */

  lazy val rulePatternFunAbs = rule[Exp] {
    case PatternFunAbs(p @ PatternProd(ps), e) =>
      val idn = SymbolTable.next()
      FunAbs(IdnDef(idn, None),
        ExpBlock(
          ps.zipWithIndex.map{
            case (p1, idx) => p1 match {
              case PatternIdn(idn1) => Bind(idn1, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
              case p2: PatternProd  => PatternBind(p2, RecordProj(IdnExp(IdnUse(idn)), s"_${idx + 1}"))
            }
          }, e))
  }

  /** De-sugar pattern generators.
    * e.g. `for ( ((a,b),c) <- X, ...` becomes `for (x <- X, ((a,b),c) := x, ...)`
    */

  object RulePatternGen {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternGen](qs, { case g: PatternGen => g})
  }

  lazy val rulePatternGen = rule[Comp] {
    case Comp(m, RulePatternGen(r, PatternGen(p, u), s), e) =>
      val idn = SymbolTable.next()
      Comp(m, r ++ Seq(Gen(IdnDef(idn, None), u), PatternBind(p, IdnExp(IdnUse(idn)))) ++ s, e)
  }

  /** De-sugar pattern binds inside expression blocks.
    * e.g. `{ ((a,b),c) = X; ... }` becomes `{ (a,b) = X._1; c = X._2; ... }`
    */

  lazy val rulePatternBindExpBlock = rule[ExpBlock] {
    case ExpBlock(PatternBind(PatternProd(ps), u) :: rest, e) =>
      ExpBlock(ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(u, s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(u, s"_${idx + 1}"))
        }
      } ++ rest, e)
  }

  /** De-sugar pattern binds inside comprehension qualifiers.
    * e.g. `for (..., ((a,b),c) = X, ...)` becomes `for (..., (a,b) = X._1, c = X._2, ...)`
    */

  object RulePatternBindComp {
    def unapply(qs: Seq[Qual]) = splitWith[Qual, PatternBind](qs, { case b: PatternBind => b})
  }

  lazy val rulePatternBindComp = rule[Comp] {
    case Comp(m, RulePatternBindComp(r, PatternBind(PatternProd(ps), u), s), e) =>
      Comp(m, r ++ ps.zipWithIndex.map {
        case (p, idx) => p match {
          case PatternIdn(idn) => Bind(idn, RecordProj(u, s"_${idx + 1}"))
          case p1: PatternProd => PatternBind(p1, RecordProj(u, s"_${idx + 1}"))
        }
      } ++ s, e)
  }

  /** De-sugar expression blocks by removing the binds one-at-a-time.
    */

  lazy val ruleExpBlocks = rule[ExpBlock] {
    case ExpBlock(Bind(IdnDef(x, _), u) :: rest, e) =>
      val strategy = everywhere(rule[Exp] {
        case IdnExp(IdnUse(`x`)) => deepclone(u)
      })
      val nrest = rewrite(strategy)(rest)
      val ne = rewrite(strategy)(e)
      ExpBlock(nrest, ne)
  }

  /** De-sugar expression blocks without bind statements.
    */

  lazy val ruleEmptyExpBlock = rule[Exp] {
    case ExpBlock(Nil, e) => e
  }

}
