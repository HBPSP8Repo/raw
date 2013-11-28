package raw.algebra

import raw._

case class Function(vs: Set[Variable], e: Expression)
case class FunctionVars(vs: Set[Variable], ps: Set[Variable])

/** Algebra
 */
sealed abstract class Algebra

case object Empty extends Algebra

case class Reduce(m: Monoid, e: Function, p: Function, X: Algebra) extends Algebra

case class Nest(m: Monoid, e: Function, f: FunctionVars, p: Function, g: FunctionVars, X: Algebra) extends Algebra

case class Selection(p: Function, X: Expression) extends Algebra

case class Join(p: Function, X: Algebra, Y: Algebra) extends Algebra

case class Unnest(path: Function, p: Function, X: Algebra) extends Algebra

case class OuterJoin(p: Function, X: Algebra, Y: Algebra) extends Algebra

case class OuterUnnest(path: Function, p: Function, X: Algebra) extends Algebra

/** FunctionPrettyPrinter
 */

object FunctionPrettyPrinter {
  def apply(f: Function) = f match {
    case Function(vs, e) =>  "\\(" + vs.map(v => ExpressionPrettyPrinter(v)).mkString(", ") + ") : " + ExpressionPrettyPrinter(e)
  }
}

/** FunctionVarsPrettyPrinter
 */

object FunctionVarsPrettyPrinter {
  def apply(f: FunctionVars) = f match {
    case FunctionVars(vs, ps) => "\\(" + vs.map(v => ExpressionPrettyPrinter(v)).mkString(", ") + ") : " + ps.map(v => v.toString()).mkString(", ") 
  }
}

/** AlgebraPrettyPrinter
 */

object AlgebraPrettyPrinter {
  def apply(a: Algebra, pre: String = ""): String =  pre + (a match {
    case Reduce(m, e, p, x) => "Reduce " + MonoidPrettyPrinter(m) + " [ e = " + FunctionPrettyPrinter(e) + " ] [ p = " + FunctionPrettyPrinter(p) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Nest(m, e, f, p, g, x) => "Nest " + MonoidPrettyPrinter(m) + " [ e = " + FunctionPrettyPrinter(e) + " ] [ f = " + FunctionVarsPrettyPrinter(f) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] [ g = " + FunctionVarsPrettyPrinter(g) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Selection(p, x) => "Selection [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + ExpressionPrettyPrinter(x, pre + "  | ")
    case Join(p, x, y) => "Join [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case Unnest(path, p, x) => "Unnest [ path = " + FunctionPrettyPrinter(path) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case OuterJoin(p, x, y) => "OuterJoin [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case OuterUnnest(path, p, x) => "OuterUnnest [ path = " + FunctionPrettyPrinter(path) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Empty => ""
  })   
}