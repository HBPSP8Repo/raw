package raw.algebra

import raw._

case class Function(vs: Set[calculus.canonical.Variable], e: Expression)
case class FunctionVars(vs: Set[calculus.canonical.Variable], ps: Set[calculus.canonical.Variable])
case class FunctionPath(vs: Set[calculus.canonical.Variable], p: calculus.canonical.Path)

/** Algebra
 */
sealed abstract class Algebra

case object Empty extends Algebra

case class Scan(name: String) extends Algebra

case class Reduce(m: Monoid, e: Function, p: Function, X: Algebra) extends Algebra

case class Nest(m: Monoid, e: Function, f: FunctionVars, p: Function, g: FunctionVars, X: Algebra) extends Algebra

case class Select(p: Function, X: Algebra) extends Algebra

case class Join(p: Function, X: Algebra, Y: Algebra) extends Algebra

case class Unnest(path: FunctionPath, p: Function, X: Algebra) extends Algebra

case class OuterJoin(p: Function, X: Algebra, Y: Algebra) extends Algebra

case class OuterUnnest(path: FunctionPath, p: Function, X: Algebra) extends Algebra

/** FunctionPrettyPrinter
 */

object FunctionPrettyPrinter {
  def apply(f: Function) = f match {
    case Function(vs, e) =>  "\\(" + vs.map(v => calculus.canonical.CalculusPrettyPrinter(v)).mkString(", ") + ") : " + ExpressionPrettyPrinter(e)
  }
}

/** FunctionVarsPrettyPrinter
 */

object FunctionVarsPrettyPrinter {
  def apply(f: FunctionVars) = f match {
    case FunctionVars(vs, ps) => "\\(" + vs.map(v => calculus.canonical.CalculusPrettyPrinter(v)).mkString(", ") + ") : " + ps.map(v => calculus.canonical.CalculusPrettyPrinter(v)).mkString(", ") 
  }
}

/** FunctionPathPrettyPrinter
 */

object FunctionPathPrettyPrinter {
  def apply(f: FunctionPath) = f match {
    case FunctionPath(vs, p) => "\\(" + vs.map(v => calculus.canonical.CalculusPrettyPrinter(v)).mkString(", ") + ") : " + calculus.canonical.PathPrettyPrinter(p) 
  }
}

/** AlgebraPrettyPrinter
 */

object AlgebraPrettyPrinter {
  def apply(a: Algebra, pre: String = ""): String =  pre + (a match {
    case Scan(name) => "Scan " + name
    case Reduce(m, e, p, x) => "Reduce " + MonoidPrettyPrinter(m) + " [ e = " + FunctionPrettyPrinter(e) + " ] [ p = " + FunctionPrettyPrinter(p) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Nest(m, e, f, p, g, x) => "Nest " + MonoidPrettyPrinter(m) + " [ e = " + FunctionPrettyPrinter(e) + " ] [ f = " + FunctionVarsPrettyPrinter(f) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] [ g = " + FunctionVarsPrettyPrinter(g) + " ]" + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Select(p, x) => "Select [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Join(p, x, y) => "Join [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case Unnest(path, p, x) => "Unnest [ path = " + FunctionPathPrettyPrinter(path) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case OuterJoin(p, x, y) => "OuterJoin [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ") + "\n" + AlgebraPrettyPrinter(y, pre + "  | ")
    case OuterUnnest(path, p, x) => "OuterUnnest [ path = " + FunctionPathPrettyPrinter(path) + " ] [ p = " + FunctionPrettyPrinter(p) + " ] " + "\n" + AlgebraPrettyPrinter(x, pre + "  | ")
    case Empty => "Empty"
  })   
}