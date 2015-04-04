package raw.psysicalalgebra

import raw.algebra.Expressions.Exp
import raw.algebra.ExpressionsPrettyPrinter
import raw.{Monoid, PrettyPrinter}

object PhysicalAlgebraPrettyPrinter extends PrettyPrinter {

  import PhysicalAlgebra._

  def apply(n: PhysicalAlgebraNode): String =
    super.pretty(show(n)).layout

  private def scan(opType: String, name: String): Doc = s"${opType}_scan" <> parens( s""""$name"""")

  private def reduce(opType: String, m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_reduce" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(p), nest(show(child))), comma))))

  private def nest(opType: String, m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_nest" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(f), ExpressionsPrettyPrinter(p), ExpressionsPrettyPrinter(g), nest(show(child))), comma))))

  private def select(opType: String, p: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_select" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(child))), comma))))

  private def join(opType: String, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode): Doc =
    s"${opType}_join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))

  private def unnest(opType: String, path: Exp, pred: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_unnest" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))

  private def outerJoin(opType: String, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode): Doc =
    s"${opType}_outer_join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))

  private def outerUnnest(opType: String, path: Exp, pred: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_outer_unnest" <> parens(group(nest(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))

  private def merge(opType: String, m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) =
    s"${opType}_merge" <> parens(nest(group(lsep(List(monoid(m), nest(show(left)), nest(show(right))), comma))))


  def show(a: PhysicalAlgebraNode): Doc = a match {
    case SparkScan(name, _) => scan("spark", name)
    case ScalaScan(name, _) => scan("scala", name)

    case SparkReduce(m, e, p, child) => reduce("spark", m, e, p, child)
    case ScalaReduce(m, e, p, child) => reduce("scala", m, e, p, child)

    case SparkNest(m, e, f, p, g, child) => nest("spark", m, e, f, p, g, child)
    case ScalaNest(m, e, f, p, g, child) => nest("scala", m, e, f, p, g, child)

    case SparkSelect(p, child) => select("spark", p, child)
    case ScalaSelect(p, child) => select("scala", p, child)

    case SparkJoin(p, left, right) => join("spark", p, left, right)
    case ScalaJoin(p, left, right) => join("scala", p, left, right)

    case SparkUnnest(path, pred, child) => unnest("spark", path, pred, child)
    case ScalaUnnest(path, pred, child) => unnest("scala", path, pred, child)

    case SparkOuterJoin(p, left, right) => outerJoin("spark", p, left, right)
    case ScalaOuterJoin(p, left, right) => outerJoin("scala", p, left, right)

    case SparkOuterUnnest(path, pred, child) => outerUnnest("spark", path, pred, child)
    case ScalaOuterUnnest(path, pred, child) => outerUnnest("scala", path, pred, child)

    case SparkMerge(m, left, right) => merge("spark", m, left, right)
    case ScalaMerge(m, left, right) => merge("scala", m, left, right)
  }
}