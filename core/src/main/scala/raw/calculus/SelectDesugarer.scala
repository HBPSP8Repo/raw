package raw
package calculus

/** Desugar Select (w/o group bys) into a for comprehension
  */
class SelectDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._
  import Calculus._

  def strategy = translate

  private def translate = manytd(selectToComp)

  private lazy val selectToComp = rule[Exp] {
    case s @ Select(from, distinct, None, proj, where, None, None) =>
      val whereq = if (where.isDefined) Seq(where.head) else Seq()
      analyzer.tipe(s) match {
        case analyzer.ResolvedType(CollectionType(m, _)) => Comp(m, from ++ whereq, proj)
      }
  }

}