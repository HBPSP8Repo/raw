//package raw
//package executor
//
//import com.typesafe.scalalogging.LazyLogging
//import shapeless.HMap
//
//case class ExecutorError(err: String) extends RawException(err)
//
//object Executor extends LazyLogging {
//
//  import algebra._
//  import LogicalAlgebra._
//  import Expressions._
//
//  def makeProduct(x: Any, y: Any): Map[String, Any] = Map("_1" -> x, "_2" -> y)
//
//  // TODO: Remove and swap for a pretty printer?
//    def show(x: Any): String = x match {
//      case _: Boolean => x.toString
//      case _: Float => x.toString
//      case _: String => x.toString
//      case xy: Tuple2[String, Any] => (xy._1.toString, show(xy._2)).toString()
//      case m: Map[String, Any] => "{" ++ m.map(kv => (kv._1, show(kv._2))).mkString(", ") ++ "}"
//      case l: List[Any] => "[" ++ l.map(show).mkString(", ") ++ "]"
//      case s: Set[Any] => "(" ++ s.map(show).mkString(", ") ++ ")"
//      case _ => "null"
//    }
//
//  def apply(root: LogicalAlgebraNode, sources: HMap[RawSources]): Either[QueryError, Any] = {
//
//    def toOperator(opNode: LogicalAlgebraNode): ScalaOperator = opNode match {
//      case Join(p, left, right) => new JoinOperator(p, toOperator(left), toOperator(right))
//      case OuterJoin(p, left, right) => new OuterJoinOperator(p, toOperator(left), toOperator(right))
//      case Select(p, source) => new SelectOperator(p, toOperator(source))
//      case Nest(m, e, f, p, g, child) => new NestOperator(m, e, f, p, g, toOperator(child))
//      case OuterUnnest(path, p, child) => new OuterUnnestOperator(path, p, toOperator(child))
//      case Unnest(path, p, child) => new UnnestOperator(path, p, toOperator(child))
//      case _: Merge => ???
//      case Reduce(m, exp, ps, source) => new ReduceOperator(m, exp, ps, toOperator(source))
//      //case Scan(name, tipe) => new ScanOperator(sources.get(name))
//    }
//
//    logger.debug("\n==========\n" + root + "\n" + LogicalAlgebraPrettyPrinter(root) + "\n============")
//    val operator = toOperator(root)
//    Right(operator.value)
//  }
//
//  private def evalPredicate(p: Exp, value: Any): Boolean = {
//    expEval(p, value).asInstanceOf[Boolean]
//  }
//
//  private def toBool(v: Any): Boolean = v match {
//    case b: Boolean => b
//    case _ => false
//  }
//
//  private def expEval(exp: Exp, env: Any): Any = {
//    //logger.debug("eval " + AlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.toString + ")")
//    val result = exp match {
//      case Null                                         => null
//      case BoolConst(v)                                 => v
//      case IntConst(v)                                  => v.toInt
//      case FloatConst(v)                                => v.toFloat
//      case StringConst(v)                               => v
//      case Arg                                          => env
//      case RecordCons(attributes)                       => attributes.map(att => (att.idn, expEval(att.e, env))).toMap
//      case RecordProj(e, idn)                           => expEval(e, env) match {
//        case v: Map[String, Any] => v(idn)
//        //
//        case x                   => throw ExecutorError(s"cannot do a record projection from $e with $x")
//      }
//      case ZeroCollectionMonoid(m)                      => zeroEval(m)
//      case ConsCollectionMonoid(m: CollectionMonoid, e) => consCollectionEval(m)(expEval(e, env))
//      case MergeMonoid(m: CollectionMonoid, e1, e2)     => mergeEval(m)(expEval(e1, env), expEval(e2, env))
//      case MergeMonoid(m: BoolMonoid, e1, e2)           => mergeBoolEval(m)(expEval(e1, env), expEval(e2, env))
//      case UnaryExp(op, e)                              => unaryOpEval(op)(expEval(e, env))
//      case IfThenElse(e1, e2, e3)                       => if (expEval(e1, env) == true) expEval(e2, env) else expEval(e3, env)
//      case BinaryExp(op, e1, e2)                        => binaryOpEval(op)(expEval(e1, env), expEval(e2, env))
//    }
//    //logger.debug("eval " + AlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.toString + ") ===> " + (if (result == null) "<null>" else result.toString))
//    result
//  }
//
//  private def zeroEval(m: Monoid): Any = m match {
//    case _: SetMonoid => Set()
//    case _: ListMonoid => List()
//    case _: BagMonoid => ???
//    case _: AndMonoid => true
//    case _: OrMonoid => false
//    case _: SumMonoid => 0
//    case _: MultiplyMonoid => 1
//    case _: MaxMonoid => 0 // TODO
//  }
//
//  private def consCollectionEval(m: CollectionMonoid): Any => Any = m match {
//    case _: SetMonoid => (e: Any) => Set(e)
//    case _: ListMonoid => (e: Any) => List(e)
//    case _: BagMonoid => (e: Any) => ???
//  }
//
//  private def mergeBoolEval(m: BoolMonoid): (Any, Any) => Any = m match {
//    case b: AndMonoid => (v1: Any, v2: Any) => (v1, v2) match {
//      case (b1: Boolean, b2: Boolean) => b1 && b2
//      case _ => throw ExecutorError(s"cannot perform 'and' of $v1 and $v2")
//    }
//    case b: OrMonoid => (v1: Any, v2: Any) => (v1, v2) match {
//      case (b1: Boolean, b2: Boolean) => b1 || b2
//      case _ => throw ExecutorError(s"cannot perform 'or' of $v1 and $v2")
//    }
//  }
//
//  private def mergeEval(m: CollectionMonoid): (Any, Any) => Any = m match { // TODO Iterable?
//    case _: ListMonoid => (e1: Any, e2: Any) => (e1, e2) match {
//      case (l1: List[Any], l2: List[Any]) => l1 ++ l2
//      case (l1: List[Any], l2: Set[Any]) => l1 ++ l2.toList
//      case (l1: Set[Any], l2: Set[Any]) => l1.toList ++ l2.toList
//      case (l1: Set[Any], l2: List[Any]) => l1.toList ++ l2
//      case _ => throw ExecutorError(s"building a list from $e1 and $e2 is not supported")
//    }
//    case _: SetMonoid => (e1: Any, e2: Any) => (e1, e2) match {
//      case (l1: Set[Any], l2: Set[Any]) => l1 ++ l2
//      case _ => throw ExecutorError(s"building a set from $e1 and $e2 is not supported")
//    }
//    case _: BagMonoid => throw ExecutorError("bags are not supported")
//  }
//
//  private def unaryOpEval(op: UnaryOperator): Any => Any = op match {
//    case _: Not => v: Any => v match {
//      case b: Boolean => !b
//      case f: Float => f == 0f
//      case i: Int => i == 0
//      case _ => throw ExecutorError(s"cannot compute not($v)")
//    }
//    case _: Neg => v: Any => v match {
//      case f: Float => -f
//      case i: Int => -i
//      case _ => throw ExecutorError(s"cannot compute neg($v)")
//    }
//    case _: ToBool => v: Any => v match {
//      case _: Boolean => v
//      case f: Float => f != 0f
//      case i: Int => i != 0
//      case _ => throw ExecutorError(s"cannot compute toBool($v)")
//    }
//    case _: ToInt  => v: Any => v match {
//      case true => 1
//      case false => 0
//      case f: Float => f.toInt
//      case _: Int => v
//      case _ => throw ExecutorError(s"cannot compute toNumber($v)")
//    }
//    case _: ToFloat  => v: Any => v match {
//      case true => 1f
//      case false => 0f
//      case i: Int => i.toInt
//      case _: Float => v
//      case _ => throw ExecutorError(s"cannot compute toNumber($v)")
//    }
//    case _: ToString => v: Any => v match {
//      case b: Boolean => b.toString()
//      case f: Float => f.toString()
//      case i: Int => i.toString()
//      case s: String => v
//      case _ => throw ExecutorError(s"cannot compute toString($v)")
//    }
//  }
//
//  private def binaryOpEval(op: BinaryOperator): (Any, Any) => Any = op match {
//    case _: Eq => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 == e2
//      case (e1: Int, e2: Int) => e1 == e2
//      case (e1: Boolean, e2: Boolean) => e1 == e2
//      case (e1: String, e2: String) => e1 == e2
//        // TODO: Hack
//      case (e1: Map[String, Any], e2: Map[String, Any]) => e1.keys.toList == e2.keys.toList && e1.values.toList == e2.values.toList
//        // TODO: Hack
//      case (e1: Map[String, Any], null) => e1.values.toList.contains(null)
//      case _ => throw ExecutorError(s"cannot compute eq($i1, $i2)")
//    }
//    case _: Neq => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 != e2
//      case (e1: Int, e2: Int) => e1 != e2
//      case (e1: Int, e2: Float) => e1 != e2
//      case (e1: Float, e2: Int) => e1 != e2
//      case (e1: Boolean, e2: Boolean) => e1 != e2
//      case (e1: String, e2: String) => e1 != e2
//      case _ => throw ExecutorError(s"cannot compute neq($i1, $i2)")
//    }
//    case _: Lt => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 < e2
//      case (e1: Int, e2: Int) => e1 < e2
//      case (e1: Int, e2: Float) => e1 < e2
//      case (e1: Float, e2: Int) => e1 < e2
//      case _ => throw ExecutorError(s"cannot compute lt($i1, $i2)")
//    }
//    case _: Le => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 <= e2
//      case (e1: Int, e2: Int) => e1 <= e2
//      case (e1: Int, e2: Float) => e1 <= e2
//      case (e1: Float, e2: Int) => e1 <= e2
//      case _ => throw ExecutorError(s"cannot compute le($i1, $i2)")
//    }
//    case _: Ge => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 >= e2
//      case (e1: Int, e2: Int) => e1 >= e2
//      case (e1: Int, e2: Float) => e1 >= e2
//      case (e1: Int, e2: Float) => e1 >= e2
//      case _ => throw ExecutorError(s"cannot compute ge($i1, $i2)")
//    }
//    case _: Gt => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 > e2
//      case (e1: Int, e2: Int) => e1 > e2
//      case (e1: Int, e2: Float) => e1 > e2
//      case (e1: Int, e2: Float) => e1 > e2
//      case _ => throw ExecutorError(s"cannot compute gt($i1, $i2)")
//    }
//    case _: Div => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 / e2
//      case (e1: Int, e2: Int) => e1 / e2
//      case (e1: Int, e2: Float) => e1 / e2
//      case (e1: Int, e2: Float) => e1 / e2
//      case _ => throw ExecutorError(s"cannot compute div($i1, $i2)")
//    }
//    case _: Sub => (i1: Any, i2: Any) => (i1, i2) match {
//      case (e1: Float, e2: Float) => e1 - e2
//      case (e1: Int, e2: Int) => e1 - e2
//      case (e1: Int, e2: Float) => e1 - e2
//      case (e1: Int, e2: Float) => e1 - e2
//      case _ => throw ExecutorError(s"cannot compute sub($i1, $i2)")
//    }
//  }
//
//  abstract class ScalaOperator {
//    def value: Any
//    def data: Iterable[Any] = value.asInstanceOf[Iterable[Any]]
//    def printMyOutput(data: Any) = logger.debug(s"\n$this\n" ++ show(value))
//  }
//
//  class JoinOperator(p: Exp, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "join (" + ExpressionsPrettyPrinter(p) + ") " + left + " X " + right
//    val leftData = left.value match { case x: Iterable[Any] => x ; case x => List(x) }
//    val rightData = right.value match { case x: Iterable[Any] => x ; case x => List(x) }
//    val output = for (l <- leftData; r <- rightData if expEval(p, makeProduct(l, r)) == true) yield makeProduct(l, r)
//    printMyOutput(output)
//    def value = output
//  }
//
//
//  class OuterJoinOperator(p: Exp, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "outer-join (" + ExpressionsPrettyPrinter(p) + ") " + left + " X " + right
//    val leftData = left.value match { case x: Iterable[Any] => x ; case x => List(x) }
//    val rightData = right.value match { case x: Iterable[Any] => x ; case x => List(x) }
//    //val output = for (l <- leftData; r <- rightData) yield if (expEval(p, makeProduct(l, r)) == true) makeProduct(l, r) else makeProduct(l, null)
//    //val output = for (l <- leftData; r <- rightData) yield if (l == null) makeProduct(null, null) (expEval(p, makeProduct(l, r)) == true) makeProduct(l, r) else makeProduct(l, null)
//
//    val output = leftData.flatMap { l => {
//      if (l == null) List(makeProduct(null, null))
//      else {
//        val ok = rightData.filter { r => evalPredicate(p, makeProduct(l, r))}
//        if (ok.isEmpty) List(makeProduct(l, null)) else ok.map { r => makeProduct(l, r)}
//      }
//    }
//    }
//    printMyOutput(output)
//    def value = output
//  }
//
//
//
//  /** Select Operator
//    */
//  class SelectOperator(p: Exp, child: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "select (" + ExpressionsPrettyPrinter(p) + ") " + child
//    val output = child.data.filter(evalPredicate(p, _) == true)
//    printMyOutput(output)
//    def value = output
//  }
//
//
//
//  class UnnestOperator(path: Exp, p: Exp, child: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "unnest (" + path + ", " + ExpressionsPrettyPrinter(p) + ") " + child
//    private val output = for (v <- child.data; pathV <- expEval(path, v).asInstanceOf[Iterable[_]] if evalPredicate(p, makeProduct(v, pathV))) yield makeProduct(v, pathV)
//    printMyOutput(output)
//    def value = output
//  }
//
//  class OuterUnnestOperator(path: Exp, p: Exp, child: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "outer-unnest (" + path + ", " + ExpressionsPrettyPrinter(p) + ") " + child
//
//    //private val output = for (v <- child.data; pathV <- expEval(path, v).asInstanceOf[Iterable[Any]]) yield if (evalPredicate(p, makeProduct(v, pathV))) makeProduct(v, pathV) else null
//    val output = child.data.flatMap { l => {
//      if (l == null) List(makeProduct(null, null))
//      else {
//        val ok = expEval(path, l).asInstanceOf[Iterable[_]].filter { r => evalPredicate(p, makeProduct(l, r))}
//        if (ok.isEmpty) List(makeProduct(l, null)) else ok.map { r => makeProduct(l, r)}
//      }
//    }
//  }
//    printMyOutput(output)
//    def value() = output
//  }
//
//  class NestOperator(m: Monoid, e: Exp, f: Exp, p: Exp, g: Exp, child: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "nest (" + List(m, ExpressionsPrettyPrinter(e), f, ExpressionsPrettyPrinter(p), g, child).mkString(", ") + ")"
//
//    // build a ProductCons of the f variables and group rows of the child by its value
//    private val valuesG: Map[Any, Iterable[Any]] = child.data.groupBy({l => expEval(f, l)})
//    // filter out with predicate p
//    private val valuesS: Map[Any, Iterable[Any]] = valuesG.map(kv => (kv._1, kv._2.filter(v => evalPredicate(p, v))))
//    // replace the set of rows associated to each "group by value" by the value of the expression e when applied to each row
//    // unless the ProductCons(g) evaluates to null, in which case we put the zero.
//    private val nullG = BinaryExp(Eq(), g, Null)
//    private val valuesE: Map[Any, Iterable[Any]] = valuesS.map(p => (p._1, p._2.map({x: Any => if (expEval(nullG, x) == true) zeroEval(m) else expEval(e, x)})))
//    // apply the reduce operation to the set of rows of each group by value
//    private val values: Map[Any, Any] = valuesE.map(p => (p._1, p._2.foldLeft(zeroEval(m))({(x: Any, y: Any) => doReduce(m, x, y)})))
//    // make a product of the group by value and the reduced operation result
//    private val output: List[Map[String, Any]] = values.toList.map{p => makeProduct(p._1, p._2)}
//    printMyOutput(output)
//    def value() = output
//  }
//
//  private def doReduce(m: Monoid, v1: Any, v2: Any): Any = (m, v1, v2) match {
//    case (_: ListMonoid, v: Any, l: List[Any]) => List(v) ++ l
//    case (_: SetMonoid, v: Any, s: Set[Any]) => Set(v) ++ s
//    case (_: AndMonoid, v1: Boolean, v2: Boolean) => v1 && v2
//    case (_: OrMonoid, v1: Boolean, v2: Boolean) => v1 && v2
//    case (_: SumMonoid, v1: Float, v2: Float) => v1 + v2
//    case (_: SumMonoid, v1: Int, v2: Int) => v1 + v2
//    case (_: MultiplyMonoid, v1: Float, v2: Float) => v1 * v2
//    case (_: MultiplyMonoid, v1: Int, v2: Int) => v1 * v2
//    case (_: MaxMonoid, v1: Float, v2: Float) => math.max(v1, v2)
//    case (_: MaxMonoid, v1: Int, v2: Int) => math.max(v1, v2)
//    case _ => throw ExecutorError(s"cannot reduce($m, $v1, $v2)")
//  }
//
//  private def monoidOp(m: Monoid): (Any, Any) => Any = m match {
//    case _: ListMonoid => (l, v) => l.asInstanceOf[List[Any]] :+ v
//    case _: SetMonoid => (s, v) => s.asInstanceOf[Set[Any]] + v
//    case _: AndMonoid => (b1, b2) => b1.asInstanceOf[Boolean] && b2.asInstanceOf[Boolean]
//    case _: OrMonoid => (b1, b2) => b1.asInstanceOf[Boolean] || b2.asInstanceOf[Boolean]
//    case _: SumMonoid => (v1, v2) => (v1, v2) match {
//      case (f1: Float, f2: Float) => f1 + f2
//      case (f1: Int, f2: Float) => f1 + f2
//      case (f1: Float, f2: Int) => f1 + f2
//      case (f1: Int, f2: Int) => f1 + f2
//    }
//    case _: MultiplyMonoid => (v1, v2) => (v1, v2) match {
//      case (f1: Float, f2: Float) => f1 * f2
//      case (f1: Int, f2: Float) => f1 * f2
//      case (f1: Float, f2: Int) => f1 * f2
//      case (f1: Int, f2: Int) => f1 * f2
//    }
//    case _: MaxMonoid => (v1, v2) => (v1, v2) match {
//      case (f1: Float, f2: Float) => math.max(f1, f2)
//      case (f1: Int, f2: Float) => math.max(f1, f2)
//      case (f1: Float, f2: Int) => math.max(f1, f2)
//      case (f1: Int, f2: Int) => math.max(f1, f2)
//
//    }
//  }
//
//  /** Reduce Operator
//    */
//  class ReduceOperator(m: Monoid, e: Exp, p: Exp, source: ScalaOperator) extends ScalaOperator {
//
//    override def toString() = "reduce (" + List(m, ExpressionsPrettyPrinter(e), ExpressionsPrettyPrinter(p), source).mkString(", ") + ")"
//    val output = source.data.filter(evalPredicate(p, _)).map(expEval(e, _)).foldLeft(zeroEval(m))(monoidOp(m))
//    printMyOutput(output)
//    def value() = output
//  }
//
//  /** Scan Operator
//    */
//  class ScanOperator(it: Iterable[Any]) extends ScalaOperator {
//    override def toString() = "scan()"
//    def value() = it
//  }
//}