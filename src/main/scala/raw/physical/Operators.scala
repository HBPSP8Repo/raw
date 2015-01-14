//package raw.physical
//
//import raw._
//
//sealed abstract class Operator
//
//// TODO: Replace name by a physical reference to the data
//case class Scan(name: String) extends Operator
//
//case class Filter(p: logical.algebra.Expression, child: Operator) extends Operator
//
//case class Materializer(child: Operator) extends Operator
//
//case class Reduce(m: logical.Monoid, e: logical.algebra.Expression, child: Operator) extends Operator
//
//case class Unnest(path: logical.algebra.Path, child: Operator) extends Operator
//
//case class Join(p: logical.algebra.Expression, left: Operator, right: Operator) extends Operator
//
////case class HashJoin(, left: Operator, right: Operator) extends Operator
//
//object OperatorPrettyPrinter {
//  def apply(op: Operator, pre: String = ""): String =  pre + (op match {
//    case Scan(name) => "Scan " + name
//    case Reduce(m, e, x) => "Reduce " + logical.MonoidPrettyPrinter(m) + " [ e = " + logical.algebra.ExpressionPrettyPrinter(e) + " ] " + "\n" + OperatorPrettyPrinter(x, pre + "  | ")
//    case Filter(p, x) => "Select [ p = " + logical.algebra.ExpressionPrettyPrinter(p) + " ] " + "\n" + OperatorPrettyPrinter(x, pre + "  | ")
//    case Unnest(path, x) => "Unnest [ path = " + logical.algebra.PathPrettyPrinter(path) + " ] " + "\n" + OperatorPrettyPrinter(x, pre + "  | ")
//  })
//}
//
////
////case class Union ??
////
//
////sealed abstract class Algebra
////
////case object Empty extends Algebra 
////
////
////case class Nest(m: Monoid, e: Expression, f: List[Argument], p: Expression, g: List[Argument], X: Algebra) extends Algebra
////
////case class Join(p: Expression, X: Algebra, Y: Algebra) extends Algebra
////
////case class Unnest(path: Path, p: Expression, X: Algebra) extends Algebra
////
////case class OuterJoin(p: Expression, X: Algebra, Y: Algebra) extends Algebra
////
////case class OuterUnnest(path: Path, p: Expression, X: Algebra) extends Algebra
////
////case class Merge(m: Monoid, X: Algebra, Y: Algebra) extends Algebra
//
//object Transform {
//  
//  def apply(a: logical.algebra.Algebra): Operator = a match {
//    case logical.algebra.Scan(name) => Scan(name)
//    case logical.algebra.Select(p, child) => Filter(p, apply(child))
//    case logical.algebra.Reduce(m, e, p, child) => Reduce(m, e, Filter(p, apply(child)))
//    case logical.algebra.Unnest(path, p, child) => Filter(p, Unnest(path, apply(child)))
//    //case logical.algebra.Join(logical.Equal()) => HashJoin
//    case logical.algebra.Join(p, left, right) => Join(p, apply(left), apply(right))
//  } 
//  
//}