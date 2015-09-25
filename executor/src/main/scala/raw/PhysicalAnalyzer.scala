package raw

import raw.calculus._
import Calculus._

/** Enhances the logical tree with physical level attributes (e.g. whether the scan origin is Scala or Spark).
  */
class PhysicalAnalyzer(tree: Calculus, world: World, val isSpark: Map[String, Boolean]) extends SemanticAnalyzer(tree, world) {

  import SymbolTable._

  lazy val spark: Exp => Boolean = attr {
    case Filter(child, _) => spark(child.e)
    case ExpBlock(_, e) => spark(e)
    case Join(left, right, _)  => spark(left.e) || spark(right.e)
    case OuterJoin(left, right, _)  => spark(left.e) || spark(right.e)
    case Unnest(child, _, _) => spark(child.e)
    case OuterUnnest(child, _, _) => spark(child.e)
    case Reduce(_, child, _) => spark(child.e)
    case Nest(_, child, _, _, _) => spark(child.e)
    case IdnExp(idn) =>
      entity(idn) match {
        case DataSourceEntity(Symbol(name)) => isSpark(name)
        case VariableEntity(idn, _)         =>
          // TODO: Requires navigating the tree to find the decl (the `e`) of idn, and then ask spark(e) to it...
          ???
      }
  }

  lazy val isSource: IdnExp => Boolean = attr {
    case IdnExp(idn) => entity(idn) match {
      case _: DataSourceEntity => true
      case _                   => false
    }
  }

}
