package raw
package calculus

class StarDesugarer(val analyzer: SemanticAnalyzer) extends SemanticTransformer {

  import org.kiama.rewriting.Cloner._
  import Calculus._
  import SymbolTable._

  def strategy = manytd(desugar)

  private lazy val desugar = rule[Exp] {
    case s: Star =>
      analyzer.starEntity(s) match {
        case StarEntity(e, _) =>
          e match {
            case select: Select if select.proj eq s =>
              // SELECT * FROM ...
              logger.debug(s"select is ${CalculusPrettyPrinter(select)}")
              if (select.from.length == 1)
                IdnExp(IdnUse(select.from.head.p.get.asInstanceOf[PatternIdn].idn.idn))
              else
                RecordCons(
                  select.from.map { case g: Gen =>
                    val idn = g.p.get.asInstanceOf[PatternIdn].idn.idn
                    AttrCons(idn, IdnExp(IdnUse(idn)))
                  })
          }
      }
  }
}