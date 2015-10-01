package raw
package calculus

import org.kiama.attribution.Attribution

/** De-sugar anonymous records, e.g.
  *   for ( <- students ) yield set ( age, for ( <- professors ) yield max age )
  * becomes
  *   for ( $0 <- students ) yield set ( $0.age, for ( $1 <- professors ) yield max $1.age )
  */
class AnonGensDesugarer(val analyzer: SemanticAnalyzer) extends Attribution with SemanticTransformer {

  import org.kiama.util.UnknownEntity
  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable.AttributeEntity

  def strategy = desugar

  private lazy val desugar =
    manybu(anonRecords) <* reduce(anonGens)

  // Generate unique IDs for Gens w/o pattern
  private lazy val anonGenSymbol: Gen => Symbol = attr {
    case Gen(None, _) => SymbolTable.next()
  }

  private lazy val anonRecords = rule[Exp] {
    case idnExp: IdnExp if analyzer.attributeEntity(idnExp) != UnknownEntity() =>
      analyzer.attributeEntity(idnExp) match {
        case AttributeEntity(att, g, idx) => RecordProj(IdnExp(IdnUse(anonGenSymbol(g).idn)), att.idn)
      }
  }

  private lazy val anonGens = rule[Gen] {
    case g @ Gen(None, e) => Gen(Some(PatternIdn(IdnDef(anonGenSymbol(g).idn))), e)
  }

}