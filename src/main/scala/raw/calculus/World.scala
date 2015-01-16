package raw.calculus

import org.kiama.util.Message

import raw.Type
import SymbolTable.ClassEntity

class World(val userTypes: Map[String, Type], val catalog: Set[ClassEntity]) extends Unnester {

  def process(ast: Calculus.Comp): Either[Seq[Message], CanonicalCalculus.Comp] = {
    val syntaxErrors = errors(ast)
    if (syntaxErrors.length == 0)
      Left(syntaxErrors)
    else
      Right(canonize(ast))
  }

}

object World {
  
  def newWorld(userTypes: Map[String, Type] = Map(), catalog: Set[ClassEntity] = Set()) =
    new World(userTypes, catalog)

}
