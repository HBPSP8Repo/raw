package raw
package algebra

import org.kiama.attribution.Attribution

case class TyperError(err: String) extends RawException(err)

class Typer(tree: Algebra.Algebra, world: World) extends Attribution {

  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import Algebra._

  /** The type errors for the tree.
    */
  lazy val errors: Messages =
    collectmessages(tree) {
      case n => check(n) {
        // Algebra nodes in the middle of the tree must be collections
        case n: AlgebraNode if !tree.isRoot(n) && !isCollection(n) => message(n, s"collection type required for non-root algebra node")

        // Merging collections of different types
        case Merge(_, left, right) if tipe(left) != tipe(right) => message(right, s"expected ${tipe(left)} but got ${tipe(right)}")

        // Predicates must be boolean expressions
        case Join(p, _, child)          if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case OuterJoin(p, _, child)     if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case Select(p, child)           if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case Unnest(_, p, child)        if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case OuterUnnest(_, p, child)   if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case Reduce(_, _, p, child)     if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")
        case Nest(_, _, _, p, _, child) if expressionType(child)(p) != BoolType() => message(p, s"expected predicate but got ${expressionType(child)(p)}")

        // Paths must be formed of either record projections or arguments
        case Unnest(path, _, _)      if !isPath(path) => message(path, s"expected path but got ${path}")
        case OuterUnnest(path, _, _) if !isPath(path) => message(path, s"expected path but got ${path}")

        // Check that "group by" and "null" variables in Nest are correct
//        case Nest(_, _, f, _, g, _) if false => ???
//
//        case tree.parent.pair(e, p) => p match {
//          case _: ProductProj => then type of e must be product type?
//
//
//        }  ProductProj(e, idx)                     => expressionType(n)(e) match {
//          case ProductType(tipes) => noMessages
//          case t                  => throw TyperError(s"Product type expected but got $t")
//        }
//        case RecordProj(e, idn)                      => expressionType(n)(e) match {
//          case RecordType(atts) => atts.collectFirst { case AttrType(`idn`, t) => t}.head
//          case t                => throw TyperError(s"Record type expected but got $t")
//        }
      }
    }

  // TODO: Instead of exceptions in expressionType, I should be returning UnknownType()

  def isCollection(n: AlgebraNode) = tipe(n) match {
    case _: CollectionType => true
    case _                 => false
  }

  /** Checks whether an expression is a path. */
  def isPath(e: Exp): Boolean = e match {
    case _: Arg              => true
    case RecordProj(e1, idn) => isPath(e1)
    case _                   => false
  }


  lazy val expected: AlgebraNode => Exp => Type = paramAttr {
    n => {
      case tree.parent.pair(e: Exp, p: Exp) => p match {
        case _: ProductProj => BoolType()
      }
    }
  }

  /** ... */
  lazy val expressionType: AlgebraNode => Exp => Type = paramAttr {
    n => {
          case Null                                    => UnknownType()
          case _: BoolConst                            => BoolType()
          case _: IntConst                             => IntType()
          case _: FloatConst                           => FloatType()
          case _: StringConst                          => StringType()
          case Arg(idx)                                => tipe(n) match {
            case CollectionType(_, ProductType(tipes)) => tipes(idx)
            case CollectionType(_, t) if idx == 0      => t  // Arg(0) is valid even for non-product types (e.g. the output of a Scan)
            case t                                     => UnknownType()
          }
          case ProductProj(e, idx)                     => expressionType(n)(e) match {
            case ProductType(tipes) => tipes(idx)
            case t                  => throw TyperError(s"Product type expected but got $t")
          }
          case ProductCons(es)                         => ProductType(es.map(expressionType(n)))
          case RecordProj(e, idn)                      => expressionType(n)(e) match {
            case RecordType(atts) => atts.collectFirst { case AttrType(`idn`, t) => t}.head
            case t                => throw TyperError(s"Record type expected but got $t")
          }
          case RecordCons(atts)                        => RecordType(atts.map(att => AttrType(att.idn, expressionType(n)(att.e))))
          case IfThenElse(_, e2, _)                    => expressionType(n)(e2)
          case BinaryExp(_: ComparisonOperator, _, _)  => BoolType()
          case BinaryExp(_: EqualityOperator, _, _)    => BoolType()
          case BinaryExp(_: ArithmeticOperator, e1, _) => expressionType(n)(e1)
          case UnaryExp(_: Not, _)                     => BoolType()
          case UnaryExp(_: Neg, e)                     => expressionType(n)(e)
          case UnaryExp(_: ToBool, _)                  => BoolType()
          case UnaryExp(_: ToInt, _)                   => IntType()
          case UnaryExp(_: ToFloat, _)                 => FloatType()
          case UnaryExp(_: ToString, _)                => StringType()
          case ZeroCollectionMonoid(m)                 => CollectionType(m, UnknownType())
          case ConsCollectionMonoid(m, e)              => CollectionType(m, expressionType(n)(e))
          case MergeMonoid(_, e1, _)                   => expressionType(n)(e1)
        }
      }

  def makeProductType(a: Type, b: Type) = (a, b) match {
    case (ProductType(t1), ProductType(t2)) => ProductType(t1 ++ t2)
    case (ProductType(t1), t2)              => ProductType(t1 ++ Seq(t2))
    case (t1, ProductType(t2))              => ProductType(Seq(t1) ++ t2)
    case (t1, t2)                           => ProductType(Seq(t1, t2))
  }

  def maxMonoid(m1: CollectionMonoid, m2: CollectionMonoid) = (m1, m2) match {
    case (_: SetMonoid, _) | (_, _: SetMonoid) => SetMonoid()
    case (_: BagMonoid, _) | (_, _: BagMonoid) => BagMonoid()
    case _                                     => ListMonoid()
  }

  /** Algebra type.
    * Rules described in [1] page 29.
    */
  lazy val tipe: AlgebraNode => Type = attr {
    case Scan(name)              => world.getSource(name).tipe

    /** Rule T17 */
    case Join(_, left, right) => {
      (tipe(left),  tipe(right)) match {
        case (CollectionType(m1, t1), CollectionType(m2, t2)) => CollectionType(maxMonoid(m1, m2), makeProductType(t1, t2))
        case _                                                => UnknownType()
      }
    }
    case OuterJoin(_, left, right) => {
      (tipe(left),  tipe(right)) match {
        case (CollectionType(m1, t1), CollectionType(m2, t2)) => CollectionType(maxMonoid(m1, m2), makeProductType(t1, t2))
        case _                                                => UnknownType()
      }
    }

    /** Rule T18 */
    case Select(_, child) => tipe(child)

    /** Rule T19 */
    case Unnest(path, _, child) => {
      (tipe(child), expressionType(child)(path)) match {
        case (CollectionType(m1, t1), CollectionType(m2, t2)) => CollectionType(maxMonoid(m1, m2), makeProductType(t1, t2))
        case _                                                => UnknownType()
      }
    }
    case OuterUnnest(path, _, child) => {
      (tipe(child), expressionType(child)(path)) match {
        case (CollectionType(m1, t1), CollectionType(m2, t2)) => CollectionType(maxMonoid(m1, m2), makeProductType(t1, t2))
        case _                                                => UnknownType()
      }
    }

    /** Rule T20 */
    case Reduce(_: PrimitiveMonoid, e, _, child)  => expressionType(child)(e)
    case Reduce(m: CollectionMonoid, e, _, child) => CollectionType(m, expressionType(child)(e))

    /** Rule T21 */
    case Nest(m, e, _, _, _, child) => tipe(child) match {
      case CollectionType(m1, t1) => CollectionType(m1, makeProductType(t1, expressionType(child)(e)))
      case _                      => UnknownType()
    }

    /** Rule for Merge */
    case Merge(_, left, right) => tipe(left)
  }
}
