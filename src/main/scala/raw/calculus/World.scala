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

  def typeOf(entity: String): Type = {
    println("typeOf(" + entity + ")")
    val matches = catalog.filter({ p => p match { case c: ClassEntity => c.name == entity }})
    matches.head.t
  }

}

object World {

  def newWorld(userTypes: Map[String, Type] = Map(), catalog: Set[ClassEntity] = Set()) =
    new World(userTypes, catalog)

}
