package raw
package calculus
import scala.collection.immutable.Seq

import org.kiama.rewriting.Strategy

/** Desugar SQL to for comprehension
  */
class Translator(val analyzer: SemanticAnalyzer) extends Transformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  private def translate = reduce(selectToComp)

  private lazy val selectToComp = rule[Exp] {
    case s @ Select(from, distinct, None, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()
      analyzer.tipe(s) match {
        case CollectionType(m, _) => Comp(m, from.map { case Iterator(Some(p), e) => Gen(p, e) } ++ whereq, proj)
      }
  }

}