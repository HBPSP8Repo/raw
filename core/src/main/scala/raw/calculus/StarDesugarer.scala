package raw
package calculus

class StarDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable._

  def transform = reduce(desugar)

  private lazy val desugar = rule[Exp] {
    case s: Star =>
      analyzer.starEntity(s) match {
        case StarEntity(select, _) =>
          assert(select.proj eq s)

          // We ask the type of the SELECT directly, instead of asking the type of each projection, so that we re-use
          // whatever renaming convention for attributes that was applied before (in the SemanticAnalyzer).

          analyzer.tipe(select) match {
            case analyzer.ResolvedType(CollectionType(_, innerType)) =>
              innerType match {
                case analyzer.ResolvedType(RecordType(Attributes(atts))) =>
                  val idns = atts.map(_.idn)
                  val projs = select.from.flatMap {
                    case Gen(Some(PatternIdn(IdnDef(idn))), e) =>
                      analyzer.tipe(e) match {
                        case analyzer.ResolvedType(CollectionType(_, innerType1)) =>
                          innerType1 match {
                            case analyzer.ResolvedType(RecordType(Attributes(atts1))) =>
                              atts1.map {
                                case AttrType(idn1, _) => RecordProj(IdnExp(IdnUse(idn)), idn1)
                              }
                          }
                      }
                  }
                  assert(idns.length == projs.length)
                  RecordCons(idns.zip(projs).map { case (idn, proj) => AttrCons(idn, proj)})
                case _ =>
                  assert(select.from.length == 1)
                  IdnExp(IdnUse(select.from.head.p.get.asInstanceOf[PatternIdn].idn.idn))
              }
          }
      }
  }
}