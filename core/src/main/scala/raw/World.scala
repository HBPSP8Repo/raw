package raw

import scala.collection.immutable.Map
import calculus.Symbol

class Group(val root: Type, val tipes: Set[Type])


/** World
  */
class World(val sources: Map[String, Type] = Map(),
            val tipes: Map[Symbol, Type] = Map()) {

  val userTypes = tipes.map { case (sym, t) => sym -> new Group(t, Set(t, TypeVariable(sym))) }

}

object World {

  //type VarMap = Map[Symbol, Type]
  type VarMap = Map[Symbol, Group]

}