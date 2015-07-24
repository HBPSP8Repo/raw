package raw.psysicalalgebra

import raw.algebra.Expressions.Exp
import raw.algebra.ExpressionsPrettyPrinter
import raw.{Monoid, PrettyPrinter}

object PhysicalAlgebraPrettyPrinter extends PrettyPrinter {

  import PhysicalAlgebra._

  def apply(n: PhysicalAlgebraNode): String =
    super.pretty(show(n)).layout

  private[this] def scan(opType: String, name: String): Doc = s"${opType}_scan" <> parens(s""""$name"""")

  private[this] def reduce(opType: String, m: Monoid, e: Exp, p: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_reduce" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(p), nest(show(child))), comma))))

  private[this] def nest(opType: String, m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_nest" <> parens(nest(group(lsep(List(monoid(m), ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(f), ExpressionsPrettyPrinter(p), ExpressionsPrettyPrinter(g), nest(show(child))), comma))))

  private[this] def select(opType: String, p: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_select" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(child))), comma))))

  private[this] def join(opType: String, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode): Doc =
    s"${opType}_join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))

  private[this] def unnest(opType: String, path: Exp, pred: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_unnest" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))

  private[this] def outerJoin(opType: String, p: Exp, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode): Doc =
    s"${opType}_outer_join" <> parens(nest(group(lsep(List(ExpressionsPrettyPrinter(p), nest(show(left)), nest(show(right))), comma))))

  private[this] def outerUnnest(opType: String, path: Exp, pred: Exp, child: PhysicalAlgebraNode): Doc =
    s"${opType}_outer_unnest" <> parens(group(nest(lsep(List(ExpressionsPrettyPrinter(path), ExpressionsPrettyPrinter(pred), nest(show(child))), comma))))

  private[this] def merge(opType: String, m: Monoid, left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) =
    s"${opType}_merge" <> parens(nest(group(lsep(List(monoid(m), nest(show(left)), nest(show(right))), comma))))

  private[this] def assign(opType: String, as: Seq[(String, PhysicalAlgebraNode)], child: PhysicalAlgebraNode) = {
    val assignDocs = as.map({ case (key, node) => text(s"$key := ") <> show(node) }).to[List]
    s"${opType}_assign" <> parens(nest(group(lsep(assignDocs ++ List(nest(show(child))), comma))))
  }

  def show(a: PhysicalAlgebraNode): Doc = a match {
    case SparkScan(lNode, name, _) => scan("spark", name)
    case ScalaScan(lNode, name, _) => scan("scala", name)

    case SparkReduce(lNode, m, e, p, child) => reduce("spark", m, e, p, child)
    case ScalaReduce(lNode, m, e, p, child) => reduce("scala", m, e, p, child)

    case SparkNest(lNode, m, e, f, p, g, child) => nest("spark", m, e, f, p, g, child)
    case ScalaNest(lNode, m, e, f, p, g, child) => nest("scala", m, e, f, p, g, child)

    case SparkSelect(lNode, p, child) => select("spark", p, child)
    case ScalaSelect(lNode, p, child) => select("scala", p, child)

    case SparkJoin(lNode, p, left, right) => join("spark", p, left, right)
    case ScalaJoin(lNode, p, left, right) => join("scala", p, left, right)

    case SparkUnnest(lNode, path, pred, child) => unnest("spark", path, pred, child)
    case ScalaUnnest(lNode, path, pred, child) => unnest("scala", path, pred, child)

    case SparkOuterJoin(lNode, p, left, right) => outerJoin("spark", p, left, right)
    case ScalaOuterJoin(lNode, p, left, right) => outerJoin("scala", p, left, right)

    case SparkOuterUnnest(lNode, path, pred, child) => outerUnnest("spark", path, pred, child)
    case ScalaOuterUnnest(lNode, path, pred, child) => outerUnnest("scala", path, pred, child)

    case SparkAssign(lNode, as, child) => assign("spark", as, child)
    case ScalaAssign(lNode, as, child) => assign("scala", as, child)

    case SparkMerge(lNode, m, left, right) => merge("spark", m, left, right)
    case ScalaMerge(lNode, m, left, right) => merge("scala", m, left, right)

    case ScalaToSparkNode(_) => ???
  }
}