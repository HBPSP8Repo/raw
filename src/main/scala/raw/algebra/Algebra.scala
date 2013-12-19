package raw.algebra

import raw._

/** Path
 */

abstract class Path
case class ArgumentPath(id: Integer) extends Path
case class InnerPath(p: Path, name: String) extends Path

/** Algebra
 */
sealed abstract class Algebra

case object Empty extends Algebra

case class Scan(name: String) extends Algebra

case class Reduce(m: Monoid, e: Expression, p: Expression, X: Algebra) extends Algebra

case class Nest(m: Monoid, e: Expression, f: List[Argument], p: Expression, g: List[Argument], X: Algebra) extends Algebra

case class Select(p: Expression, X: Algebra) extends Algebra

case class Join(p: Expression, X: Algebra, Y: Algebra) extends Algebra

case class Unnest(path: Path, p: Expression, X: Algebra) extends Algebra

case class OuterJoin(p: Expression, X: Algebra, Y: Algebra) extends Algebra

case class OuterUnnest(path: Path, p: Expression, X: Algebra) extends Algebra

/** PathPrettyPrinter
 */

object PathPrettyPrinter {
  def apply(p: Path): String = p match {
    case ArgumentPath(id) => "<arg" + id.toString() + ">"
    case InnerPath(p, name) => PathPrettyPrinter(p) + "." + name
  }
}

/** ListArgumentPrettyPrinter
 */

object ListArgumentPrettyPrinter {
  def apply(as: List[Argument]): String = 
    as.map(a => ExpressionPrettyPrinter(a)).mkString(",")
}

/** AlgebraPrettyPrinter
 */

object AlgebraPrettyPrinter {
  def apply(a: Algebra, pre: String = ""): String =  pre + (a match {
    case Scan(name) => "Scan " + name
    case Reduce(m, e, p, x) => "Reduce " + MonoidPrettyPrinter(m) + " [ e = " + ExpressionPrettyPrinter(e) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Nest(m, e, f, p, g, x) => "Nest " + MonoidPrettyPrinter(m) + " [ e = " + ExpressionPrettyPrinter(e) + " ] [ f = " +  ListArgumentPrettyPrinter(f) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] [ g = " + ListArgumentPrettyPrinter(g) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Select(p, x) => "Select [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Join(p, x, y) => "Join [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case Unnest(path, p, x) => "Unnest [ path = " + PathPrettyPrinter(path) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case OuterJoin(p, x, y) => "OuterJoin [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case OuterUnnest(path, p, x) => "OuterUnnest [ path = " + PathPrettyPrinter(path) + " ] [ p = " + ExpressionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Empty => "Empty"
  })   
}
