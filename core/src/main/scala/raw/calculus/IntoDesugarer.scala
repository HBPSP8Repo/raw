package raw
package calculus

import org.kiama.attribution.Attribution

/** De-sugar INTO, e.g.
  *   (number: 1, 2) INTO (column1: number, column2: _2)
  * becomes
  *   {
  *     $1 := (number: 1, 2)
  *     (column1: $1.number, column2: $1._2)
  *   }
  */
class IntoDesugarer(val analyzer: SemanticAnalyzer) extends Attribution with SemanticTransformer {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable.IntoAttributeEntity

  def transform = desugar

  private lazy val desugar =
    manytd(into)

  private lazy val into = rule[Exp] {
    case i @ Into(e1, e2) =>
      logger.debug(s"Processing $i")
      val sym = SymbolTable.next()

      def thisInto(idnExp: IdnExp) = analyzer.lookupAttributeEntity(idnExp) match {
        case IntoAttributeEntity(att, i1, idx) if i eq i1 => true
        case _ => false
      }

      val ne2 = rewrite(
        manybu(rule[Exp] {
          case idnExp: IdnExp if thisInto(idnExp) =>
            analyzer.lookupAttributeEntity(idnExp) match {
              case IntoAttributeEntity(att, _, _) => RecordProj(IdnExp(IdnUse(sym.idn)), att.idn)
            }
        }))(e2)

      ExpBlock(
        Seq(Bind(PatternIdn(IdnDef(sym.idn, None)), e1)),
        ne2
      )
  }

}