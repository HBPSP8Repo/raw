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
    case IdnExp(idnUse) =>
      entity(idnUse) match {
        case DataSourceEntity(Symbol(name)) => isSpark(name)
        case VariableEntity(idn, _)         => decl(idn) match {
          case Some(Bind(_, e)) => spark(e)   // TODO: This is not too precise: disregards pattern
          case Some(Gen(_, e)) => spark(e)   // TODO: This is not too precise: disregards pattern
        }
      }
    case UnaryExp(_, e) => spark(e)
    case RecordProj(e, _) => spark(e)
  }

  // TODO: Find expression of an entity; how can that be done? When we bind, we do bind to an expression.
  //       But what does it mean `e`?

  lazy val isSource: IdnExp => Boolean = attr {
    case IdnExp(idn) => entity(idn) match {
      case _: DataSourceEntity => true
      case _                   => false
    }
  }

  /** Return the type of a pattern.
    * Finds the declaration of the identifier, then its body, then types the body and projects the pattern.
    */
  lazy val patternType: Pattern => Type = attr {
    pat =>
      def findType(p: Pattern, t: Type): Option[Type] = (p, t) match {
        case (_, t1) if pat eq p => Some(t1)
        case (PatternProd(ps), t1: RecordType) =>
          ps.zip(t1.atts).flatMap { case (p1, att) => findType(p1, att.tipe) }.headOption
        case _ => None
      }

      patDecl(pat) match {
        case Some(Bind(p, e))   => findType(p, tipe(e)).get
        case Some(Gen(p, e))    => findType(p, tipe(e) match { case CollectionType(_, innerType) => innerType }).get
      }
  }


  /** Return the type of an identifier definition.
    * Finds the declaration of the identifier, then its body, then types the body and projects the pattern.
    */
  lazy val idnDefType: IdnDef => Type = attr {
    idn =>
      def findType(p: Pattern, t: Type): Option[Type] = (p, t) match {
        case (PatternIdn(idn1), t1) if idn == idn1 => Some(t1)
        case (_: PatternIdn, _)                    => None
        case (PatternProd(ps), t1: RecordType) =>
          ps.zip(t1.atts).flatMap { case (p1, att) => findType(p1, att.tipe) }.headOption
      }

      decl(idn) match {
        case Some(Bind(p, e))   => findType(p, tipe(e)).get
        case Some(Gen(p, e))    => findType(p, tipe(e) match { case CollectionType(_, innerType) => innerType }).get
      }
  }

}
