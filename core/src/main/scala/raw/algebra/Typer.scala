package raw
package algebra

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

class Typer(world: World) extends Attribution with LazyLogging {

  import Expressions._
  import LogicalAlgebra._

  def expressionType(e: Exp): Type = {
    val res = realExpressionType(e) match {
      //case UserType(idn) => world.userTypes(idn)
      case t1 => t1
    }
//    logger.info(s"Expression typed:\n$e\n=>\n$res\n")
    res
  }

  /** Actual type of an expression.
    */
  private lazy val realExpressionType: Exp => Type = attr {
    case Null => AnyType()
    case _: BoolConst => BoolType()
    case _: FloatConst => FloatType()
    case _: IntConst => IntType()
    case _: StringConst => StringType()
    case Arg(t) => t
    case rp@RecordProj(e, idn) => expressionType(e) match {
      case t: RecordType => t.atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case None =>
          logger.warn(s"Could not find attribute $idn in type $t. Outer element: $rp Returning NothingType()")
          NothingType()
      }
      case t@_ =>
        logger.warn("Record projection over invalid element. Expected: RecordType, Found: $t")
        NothingType()
    }
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, expressionType(att.e))), None)
    case IfThenElse(_, e2, _) => expressionType(e2)
    case BinaryExp(op, e1, _) => op match {
      case _: ComparisonOperator => BoolType()
      case _: ArithmeticOperator => expressionType(e1)
    }
    case UnaryExp(op, e) => op match {
      case _: Not => BoolType()
      case _: Neg => expressionType(e)
      case _: ToBool => BoolType()
      case _: ToFloat => FloatType()
      case _: ToInt => IntType()
      case _: ToString => StringType()
    }
    case MergeMonoid(_, e1, _) => expressionType(e1)
    case ConsCollectionMonoid(_: SetMonoid, e) => SetType(expressionType(e))
    case ConsCollectionMonoid(_: BagMonoid, e) => BagType(expressionType(e))
    case ConsCollectionMonoid(_: ListMonoid, e) => ListType(expressionType(e))
    case ZeroCollectionMonoid(m) => ???
  }

  private def recordType(a: Type, b: Type): RecordType =
    RecordType(List(AttrType("_1", a), AttrType("_2", b)), None)

  private def maxCollectionType(c1: CollectionType, c2: CollectionType) = (c1, c2) match {
    case (_: SetType, _) | (_, _: SetType) => SetType(recordType(c1.innerType, c2.innerType))
    case (_: BagType, _) | (_, _: BagType) => BagType(recordType(c1.innerType, c2.innerType))
    case _ => ListType(recordType(c1.innerType, c2.innerType))
  }

  def tipe(n: LogicalAlgebraNode) = {
    val res = realType(n) match {
      //case UserType(idn) => world.userTypes(idn)
      case t => t
    }
//    logger.info(s"Node type:\n$n\n=>\n$res\n")
    res
  }

  /** Operator types.
    * The rules are described in [1] page 29, but were extended to support joins between collections and primitive types.
    */
  private lazy val realType: LogicalAlgebraNode => Type = attr {
    case Scan(name, tipe) => tipe

    /** Rule T17 */
    case Join(_, left, right) =>
      (tipe(left), tipe(right)) match {
        case (c1: CollectionType, c2: CollectionType) => maxCollectionType(c1, c2)
        case (c1: CollectionType, t2) => maxCollectionType(c1, ListType(t2))
        case (t1, c2: CollectionType) => maxCollectionType(ListType(t1), c2)
        case _ => NothingType()
      }
    case OuterJoin(_, left, right) =>
      (tipe(left), tipe(right)) match {
        case (c1: CollectionType, c2: CollectionType) => maxCollectionType(c1, c2)
        case (c1: CollectionType, t2) => maxCollectionType(c1, ListType(t2))
        case (t1, c2: CollectionType) => maxCollectionType(ListType(t1), c2)
        case _ => NothingType()
      }

    /** Rule T18 */
    case Select(_, child) => tipe(child)

    /** Rule T19 */
    case Unnest(path, _, child) =>
      (tipe(child), expressionType(path)) match {
        case (c1: CollectionType, c2: CollectionType) => maxCollectionType(c1, c2)
        case _ => NothingType()
      }
    case OuterUnnest(path, _, child) =>
      (tipe(child), expressionType(path)) match {
        case (c1: CollectionType, c2: CollectionType) => maxCollectionType(c1, c2)
        case _ => NothingType()
      }

    /** Rule T20 */
    case Reduce(_: PrimitiveMonoid, e, _, child) =>
      expressionType(e)
    case Reduce(m: CollectionMonoid, e, _, child) => m match {
      case _: BagMonoid => BagType(expressionType(e))
      case _: ListMonoid => ListType(expressionType(e))
      case _: SetMonoid => SetType(expressionType(e))
    }

    /** Rule T21 */
    case Nest(_: PrimitiveMonoid, e, f, _, _, child) => tipe(child) match {
      case _: BagType => BagType(recordType(expressionType(f), expressionType(e)))
      case _: ListType => ListType(recordType(expressionType(f), expressionType(e)))
      case _: SetType => SetType(recordType(expressionType(f), expressionType(e)))
      case _ => NothingType()
    }
    case Nest(_: BagMonoid, e, f, _, _, child) => tipe(child) match {
      case _: BagType => BagType(recordType(expressionType(f), BagType(expressionType(e))))
      case _: ListType => ListType(recordType(expressionType(f), BagType(expressionType(e))))
      case _: SetType => SetType(recordType(expressionType(f), BagType(expressionType(e))))
      case _ => NothingType()
    }
    case Nest(_: ListMonoid, e, f, _, _, child) => tipe(child) match {
      case _: BagType => BagType(recordType(expressionType(f), ListType(expressionType(e))))
      case _: ListType => ListType(recordType(expressionType(f), ListType(expressionType(e))))
      case _: SetType => SetType(recordType(expressionType(f), ListType(expressionType(e))))
      case _ => NothingType()
    }
    case Nest(_: SetMonoid, e, f, _, _, child) => tipe(child) match {
      case _: BagType => BagType(recordType(expressionType(f), SetType(expressionType(e))))
      case _: ListType => ListType(recordType(expressionType(f), SetType(expressionType(e))))
      case _: SetType => SetType(recordType(expressionType(f), SetType(expressionType(e))))
      case _ => NothingType()
    }

    // TODO: Confirm with Ben
    case Assign(as, child) => tipe(child)

    /** Rule for Merge */
    case Merge(_, left, right) => tipe(left)
  }
}