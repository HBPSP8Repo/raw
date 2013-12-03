package raw.calculus.parser

/** Scope
 *  
 *  Scopes are used by the parser to store variable bindings.
 *  Scopes can be nested so that variable names can be reused in inner nestings.
 */
sealed abstract class Scope {
  var bindings = Map[String, Variable]()

  def bind(name: String, v: Variable) = bindings += (name -> v)

  def add() = new InnerScope(this)

  def exists(name: String): Boolean    

  def get(name: String): Option[Variable]
}

case class RootScope extends Scope {
  def exists(name: String): Boolean = bindings.isDefinedAt(name)

  def get(name: String): Option[Variable] = bindings.get(name)
}

case class InnerScope(parent: Scope) extends Scope {
  def exists(name: String): Boolean =
    if (bindings.isDefinedAt(name))
      true
    else
      parent.exists(name)

  def get(name: String): Option[Variable] =
    if (bindings.isDefinedAt(name))
      Some(bindings(name))
    else
      parent.get(name)
}