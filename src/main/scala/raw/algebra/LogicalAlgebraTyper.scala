package raw
package algebra

case class LogicalAlgebraTyperError(err: String) extends RawException(err)

object LogicalAlgebraTyper {

  import LogicalAlgebra._
  import Expressions._

  def apply(a: LogicalAlgebraNode): Type = a match {
    case Scan(obj, t) => t
    case Select(_, child) => apply(child)
    case Reduce(m, e, _, _) =>
      val t = ExpressionTyper(e, a)
      m match {
        case _: PrimitiveMonoid => t
        case _: BagMonoid       => BagType(t)
        case _: ListMonoid      => ListType(t)
        case _: SetMonoid       => SetType(t)
      }
    case Join(_, left, right) => makeRecordType(apply(left), apply(right))
      // this is wrong.. what is the collection?
    case OuterJoin(_, left, right) => makeRecordType(apply(left), apply(right))
      // this is wrong.. what is the collection?
    case Unnest(path, _, child) => makeRecordType(apply(child), getPathType(path, child))
      // this is wrong.. what is the collection?
    case OuterUnnest(path, _, child) => makeRecordType(apply(child), getPathType(path, child))
      // this is wrong.. what is the collection?
    case Nest(m, e, f, p, g, child) => RecordType(List(AttrType("_1", ExpressionTyper(f, child)), AttrType("_2", ExpressionTyper(e, child))))
      // this is wrong.. is nest a list of whatever?
    case Merge(_, left, _) => apply(left)
  }

  private def makeRecordType(t1: Type, t2: Type) = RecordType(List(AttrType("_1", t1), AttrType("_2", t2)))

  private def getPathType(path: Exp, n: LogicalAlgebraNode) = ExpressionTyper(path, n) match {
    case c: CollectionType => c.innerType
    case t                 => throw LogicalAlgebraTyperError(s"Unexpected type: $t")
  }

}
