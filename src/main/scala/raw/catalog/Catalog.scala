package raw.catalog

import raw.calculus._
import raw.calculus.parser._

class Catalog(val catalog: Map[String, MonoidType]) {
  private val variables = for ((name, monoidType) <- catalog) yield (name, Variable(monoidType))
  private val reverse = variables.map(_.swap)
  
  val rootScope = new RootScope()
  for ((name, variable) <- variables)
    rootScope.bind(name, variable)

  def getName(v: Variable) = reverse(v) 

}