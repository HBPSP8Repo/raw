//package raw
//package algebra
//
//case class ExpressionTyperError(err: String) extends RawException(err)
//
///** Typer for Algebra Expressions.
//  *
//  * This typer recurses from the leaf nodes, which always have well-defined type, up to the root node.
//  */
//object ExpressionTyper {
//
//  import Expressions._
//
//  def apply(e: Exp): Type = e match {
//    case Null(t)                     => t
//    case BoolConst(v)                => BoolType()
//    case IntConst(v)                 => IntType()
//    case FloatConst(v)               => FloatType()
//    case StringConst(v)              => StringType()
//    case Arg(t)                      => t
//    case RecordProj(e1, idn)         => apply(e1) match {
//      case RecordType(atts) => atts.collect { case att if att.idn == idn => att.tipe}.head
//      case t                => throw ExpressionTyperError(s"Unexpected type: $t")
//    }
//    case RecordCons(atts)            => RecordType(atts.map { case att => AttrType(att.idn, apply(att.e))})
//    case IfThenElse(_, e2, _)        => apply(e2)
//    case BinaryExp(op, e1, _)        => op match {
//      case _: ArithmeticOperator => apply(e1)
//      case _: ComparisonOperator => BoolType()
//    }
//    case ZeroCollectionMonoid(t, _)  => t
//    case ConsCollectionMonoid(m, e1) => m match {
//      case _: BagMonoid  => BagType(apply(e1))
//      case _: ListMonoid => ListType(apply(e1))
//      case _: SetMonoid  => SetType(apply(e1))
//    }
//    case MergeMonoid(_, e1, _)       => apply(e1)
//    case UnaryExp(op, e1)            => op match {
//      case _: Neg      => apply(e1)
//      case _: Not      => BoolType()
//      case _: ToBool   => BoolType()
//      case _: ToFloat  => FloatType()
//      case _: ToInt    => IntType()
//      case _: ToString => StringType()
//    }
//  }
//
//}
