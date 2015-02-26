package raw
package executor
package reference

import scala.io.BufferedSource

import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonAST._
import algebra._
import Algebra._

case class ReferenceExecutorError(err: String) extends RawException(err)

class ReferenceResult(result: Any) extends QueryResult {
  val value = result
}

object ReferenceExecutor extends Executor with LazyLogging {

  case class ProductValue(items: Seq[Any])

  def makeProduct(x: Any, y: Any): ProductValue =  (x, y) match {
    case (ProductValue(s1), ProductValue(s2)) => ProductValue(s1 ++ s2)
    case (s1: Any, ProductValue(s2)) => ProductValue(Seq(s1) ++ s2)
    case (ProductValue(s1), s2) => ProductValue(s1 :+ s2)
    case (s1, s2) => ProductValue(Seq(s1, s2))
  }

    def show(x: Any): String = x match {
      case _: Boolean => x.toString
      case _: Float => x.toString
      case _: String => x.toString
      case xy: Tuple2[String, Any] => (xy._1.toString, show(xy._2)).toString()
      case m: Map[String, Any] => "{" ++ m.map(kv => (kv._1, show(kv._2))).mkString(", ") ++ "}"
      case l: List[Any] => "[" ++ l.map(show).mkString(", ") ++ "]"
      case s: Set[Any] => "(" ++ s.map(show).mkString(", ") ++ ")"
      case p: ProductValue => "x[" ++ p.items.map(show).mkString(" . ") ++ "]x"
      case _ => "null"
    }

  def execute(root: OperatorNode, world: World): Either[QueryError, QueryResult] = {

    def dataLocDecode(t: Type, data: Iterable[Any]): Iterable[Any] = {
      def recurse(t: Type, value: Any): Any = (t, value) match {
        case (IntType(), v: Int) => v
        case (FloatType(), v: Float) => v
        case (BoolType(), v: Boolean) => v
        case (StringType(), v: String) => v
        case (r: RecordType, v: Map[_, _]) => v match {
          case x: Map[String, Any] => x.map({ c => (c._1, recurse(r.typeOf(c._1), x(c._1)))})
        }
        case (ListType(what: Type), v: List[_]) => v.map({ x: Any => recurse(what, x)})
        case (SetType(what: Type), v: Set[_]) => v.map({ x: Any => recurse(what, x)})
        case _ => throw RawExecutorRuntimeException(s"cannot parse $t in $value")
      }
      recurse(t, data) match {
        case l: List[Any] => l
        case s: Set[Any] => s
        case v => throw RawExecutorRuntimeException(s"cannot build a memory scan from $v")
      }
    }

    def loadCSV(t: Type, content: BufferedSource): Iterable[Any] = {

      def parse(t: Type, item: String): Any = t match {
        case IntType() => item.toInt
        case FloatType() => item.toFloat
        case BoolType() => item.toBoolean
        case StringType() => item
        case _ => throw RawExecutorRuntimeException(s"cannot parse $t in text/csv files")
      }
      t match {
        case ListType(RecordType(atts)) => {
          val f = (l: String) => atts.zip(l.split(",")).map { case (a, item) => (a.idn, parse(a.tipe, item))}.toMap
          content.getLines().toList.map(f)
        }
        case _ => throw RawExecutorRuntimeException(s"cannot make data source from $t")
      }
    }

    def loadJSON(t: Type, content: BufferedSource): Iterable[Any] = {
      val json: JValue = org.json4s.native.JsonMethods.parse(content.mkString)
      def convert(t: Type, item: JValue): Any = (t, item) match {
        case (IntType(), JInt(i)) => i.toInt
        case (FloatType(), JDouble(d)) => d.toFloat
        case (BoolType(), JBool(b)) => b
        case (StringType(), JString(s)) => s
        case (RecordType(atts), JObject(l)) => {
          val jMap: Map[String, JValue] = l.map { jfield: JField => (jfield._1, jfield._2)}.toMap
          val tMap: Map[String, Type] = atts.map({ aType: AttrType => (aType.idn, aType.tipe)}).toMap
          val vMap: Map[String, Any] = jMap.map({ j => (j._1, convert(tMap(j._1), j._2))})
          vMap
        }
        case (ListType(innerType), JArray(arr)) => arr.map({ jv => convert(innerType, jv)})
        case (SetType(innerType), JArray(arr)) => arr.map({ jv => convert(innerType, jv)}).toSet // TODO: correct?
      }

      convert(t, json) match {
        case l: List[Any] => l
        case s: Set[Any] => s
        case v: Any => throw RawExecutorRuntimeException(s"Cannot instantiate a scan from $v")
      }
    }

    def toOperator(opNode: OperatorNode): ScalaOperator = opNode match {
      case Join(p, left, right) => new JoinOperator(p, toOperator(left), toOperator(right))
      case OuterJoin(p, left, right) => new OuterJoinOperator(p, toOperator(left), toOperator(right))
      case Select(p, source) => new SelectOperator(p, toOperator(source))
      case Nest(m, e, f, p, g, child) => new NestOperator(m, e, f.asInstanceOf[ProductCons], p, g.asInstanceOf[ProductCons], toOperator(child))
      case OuterUnnest(path, p, child) => new OuterUnnestOperator(path, p, toOperator(child))
      case Unnest(path, p, child) => new UnnestOperator(path, p, toOperator(child))
      case _: Merge => ???
      case Reduce(m, exp, ps, source) => new ReduceOperator(m, exp, ps, toOperator(source))
      case Scan(name) => {
        val source = world.getSource(name)
        source.location match {
          case LocalFileLocation(path, "text/csv") => new ScanOperator(loadCSV(source.tipe, scala.io.Source.fromFile(path)))
          case LocalFileLocation(path, "application/json") => new ScanOperator(loadJSON(source.tipe, scala.io.Source.fromFile(path)))
          case MemoryLocation(data) => new ScanOperator(dataLocDecode(source.tipe, data))
          case loc => throw ReferenceExecutorError(s"Reference executor does not support location $loc")
        }
      }
    }

    logger.debug("\n==========\n" + root + "\n" + AlgebraPrettyPrinter.pretty(root) + "\n============")
    val operator = toOperator(root)
    Right(new ReferenceResult(operator.value))
  }

  private def evalPredicate(p: Exp, value: Any): Boolean = {
    expEval(p, value).asInstanceOf[Boolean]
  }

  private def toBool(v: Any): Boolean = v match {
    case b: Boolean => b
    case _ => false
  }

  private def expEval(exp: Exp, env: Any): Any = {
    //logger.debug("eval " + AlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.toString + ")")
    val result = exp match {
      case Null                                         => null
      case BoolConst(v)                                 => v
      case IntConst(v)                                  => v.toInt
      case FloatConst(v)                                => v.toFloat
      case StringConst(v)                               => v
      case Arg(idx)                                     => env match {
        case ProductValue(items) => items(idx)
        case v => if (idx == 0) v else throw ReferenceExecutorError(s"cannot extract $exp from $env")
      }
      case ProductCons(es)                              => ProductValue(es.map(expEval(_, env)))
      case ProductProj(e, idx)                          => expEval(e, env).asInstanceOf[Seq[Any]](idx)
      case RecordCons(attributes)                       => attributes.map(att => (att.idn, expEval(att.e, env))).toMap
      case RecordProj(e, idn)                           => expEval(e, env) match {
        case v: Map[String, Any] => v(idn)
        case _                   => throw RawExecutorRuntimeException(s"cannot do a record projection from $e")
      }
      case ZeroCollectionMonoid(m)                      => zeroEval(m)
      case ConsCollectionMonoid(m: CollectionMonoid, e) => consCollectionEval(m)(expEval(e, env))
      case MergeMonoid(m: CollectionMonoid, e1, e2)     => mergeEval(m)(expEval(e1, env), expEval(e2, env))
      case MergeMonoid(m: BoolMonoid, e1, e2)           => mergeBoolEval(m)(expEval(e1, env), expEval(e2, env))
      case UnaryExp(op, e)                              => unaryOpEval(op)(expEval(e, env))
      case IfThenElse(e1, e2, e3)                       => if (expEval(e1, env) == true) expEval(e2, env) else expEval(e3, env)
      case BinaryExp(op, e1, e2)                        => binaryOpEval(op)(expEval(e1, env), expEval(e2, env))
    }
    //logger.debug("eval " + AlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.toString + ") ===> " + (if (result == null) "<null>" else result.toString))
    result
  }

  private def zeroEval(m: Monoid): Any = m match {
    case _: SetMonoid => Set()
    case _: ListMonoid => List()
    case _: BagMonoid => ???
    case _: AndMonoid => true
    case _: OrMonoid => false
    case _: SumMonoid => 0
    case _: MultiplyMonoid => 1
    case _: MaxMonoid => 0 // TODO
  }

  private def consCollectionEval(m: CollectionMonoid): Any => Any = m match {
    case _: SetMonoid => (e: Any) => Set(e)
    case _: ListMonoid => (e: Any) => List(e)
    case _: BagMonoid => (e: Any) => ???
  }

  private def mergeBoolEval(m: BoolMonoid): (Any, Any) => Any = m match {
    case b: AndMonoid => (v1: Any, v2: Any) => (v1, v2) match {
      case (b1: Boolean, b2: Boolean) => b1 && b2
      case _ => throw RawExecutorRuntimeException(s"cannot perform 'and' of $v1 and $v2")
    }
    case b: OrMonoid => (v1: Any, v2: Any) => (v1, v2) match {
      case (b1: Boolean, b2: Boolean) => b1 || b2
      case _ => throw RawExecutorRuntimeException(s"cannot perform 'or' of $v1 and $v2")
    }
  }

  private def mergeEval(m: CollectionMonoid): (Any, Any) => Any = m match { // TODO Iterable?
    case _: ListMonoid => (e1: Any, e2: Any) => (e1, e2) match {
      case (l1: List[Any], l2: List[Any]) => l1 ++ l2
      case (l1: List[Any], l2: Set[Any]) => l1 ++ l2.toList
      case (l1: Set[Any], l2: Set[Any]) => l1.toList ++ l2.toList
      case (l1: Set[Any], l2: List[Any]) => l1.toList ++ l2
      case _ => throw RawExecutorRuntimeException(s"building a list from $e1 and $e2 is not supported")
    }
    case _: SetMonoid => (e1: Any, e2: Any) => (e1, e2) match {
      case (l1: Set[Any], l2: Set[Any]) => l1 ++ l2
      case _ => throw RawExecutorRuntimeException(s"building a set from $e1 and $e2 is not supported")
    }
    case _: BagMonoid => throw RawExecutorRuntimeException("bags are not supported")
  }

  private def unaryOpEval(op: UnaryOperator): Any => Any = op match {
    case _: Not => v: Any => v match {
      case b: Boolean => !b
      case f: Float => f == 0f
      case i: Int => i == 0
      case _ => throw RawExecutorRuntimeException(s"cannot compute not($v)")
    }
    case _: Neg => v: Any => v match {
      case f: Float => -f
      case i: Int => -i
      case _ => throw RawExecutorRuntimeException(s"cannot compute neg($v)")
    }
    case _: ToBool => v: Any => v match {
      case _: Boolean => v
      case f: Float => f != 0f
      case i: Int => i != 0
      case _ => throw RawExecutorRuntimeException(s"cannot compute toBool($v)")
    }
    case _: ToInt  => v: Any => v match {
      case true => 1
      case false => 0
      case f: Float => f.toInt
      case _: Int => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toNumber($v)")
    }
    case _: ToFloat  => v: Any => v match {
      case true => 1f
      case false => 0f
      case i: Int => i.toInt
      case _: Float => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toNumber($v)")
    }
    case _: ToString => v: Any => v match {
      case b: Boolean => b.toString()
      case f: Float => f.toString()
      case i: Int => i.toString()
      case s: String => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toString($v)")
    }
  }

  private def binaryOpEval(op: BinaryOperator): (Any, Any) => Any = op match {
    case _: Eq => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 == e2
      case (e1: Int, e2: Int) => e1 == e2
      case (e1: Boolean, e2: Boolean) => e1 == e2
      case (e1: String, e2: String) => e1 == e2
      case (e1: ProductValue, e2: ProductValue) => e1.items == e2.items
      case (e1: ProductValue, e2) => if (e2 == null) e1.items.contains(null) else throw RawExecutorRuntimeException(s"cannot compare $e1 and $e2")
      case _ => throw RawExecutorRuntimeException(s"cannot compute eq($i1, $i2)")
    }
    case _: Neq => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 != e2
      case (e1: Int, e2: Int) => e1 != e2
      case (e1: Int, e2: Float) => e1 != e2
      case (e1: Int, e2: Float) => e1 != e2
      case (e1: Boolean, e2: Boolean) => e1 != e2
      case (e1: String, e2: String) => e1 != e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute neq($i1, $i2)")
    }
    case _: Lt => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 < e2
      case (e1: Int, e2: Int) => e1 < e2
      case (e1: Int, e2: Float) => e1 < e2
      case (e1: Int, e2: Float) => e1 < e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute lt($i1, $i2)")
    }
    case _: Le => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 <= e2
      case (e1: Int, e2: Int) => e1 <= e2
      case (e1: Int, e2: Float) => e1 <= e2
      case (e1: Int, e2: Float) => e1 <= e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute le($i1, $i2)")
    }
    case _: Ge => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 >= e2
      case (e1: Int, e2: Int) => e1 >= e2
      case (e1: Int, e2: Float) => e1 >= e2
      case (e1: Int, e2: Float) => e1 >= e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute ge($i1, $i2)")
    }
    case _: Gt => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 > e2
      case (e1: Int, e2: Int) => e1 > e2
      case (e1: Int, e2: Float) => e1 > e2
      case (e1: Int, e2: Float) => e1 > e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute gt($i1, $i2)")
    }
    case _: Div => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 / e2
      case (e1: Int, e2: Int) => e1 / e2
      case (e1: Int, e2: Float) => e1 / e2
      case (e1: Int, e2: Float) => e1 / e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute div($i1, $i2)")
    }
    case _: Sub => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Float, e2: Float) => e1 - e2
      case (e1: Int, e2: Int) => e1 - e2
      case (e1: Int, e2: Float) => e1 - e2
      case (e1: Int, e2: Float) => e1 - e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute sub($i1, $i2)")
    }
  }

  abstract class ScalaOperator {
    def value: Any
    def data: Iterable[Any] = value.asInstanceOf[Iterable[Any]]
    def printMyOutput(data: Any) = logger.debug(s"\n$this\n" ++ show(value))
  }

  class JoinOperator(p: Exp, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "join (" + AlgebraPrettyPrinter.pretty(p) + ") " + left + " X " + right
    val leftData = left.value match { case x: Iterable[Any] => x ; case x => List(x) }
    val rightData = right.value match { case x: Iterable[Any] => x ; case x => List(x) }
    val output = for (l <- leftData; r <- rightData if expEval(p, makeProduct(l, r)) == true) yield makeProduct(l, r)
    printMyOutput(output)
    def value = output
  }


  class OuterJoinOperator(p: Exp, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "outer-join (" + AlgebraPrettyPrinter.pretty(p) + ") " + left + " X " + right
    val leftData = left.value match { case x: Iterable[Any] => x ; case x => List(x) }
    val rightData = right.value match { case x: Iterable[Any] => x ; case x => List(x) }
    //val output = for (l <- leftData; r <- rightData) yield if (expEval(p, makeProduct(l, r)) == true) makeProduct(l, r) else makeProduct(l, null)
    //val output = for (l <- leftData; r <- rightData) yield if (l == null) makeProduct(null, null) (expEval(p, makeProduct(l, r)) == true) makeProduct(l, r) else makeProduct(l, null)

    val output = leftData.flatMap { l => {
      if (l == null) List(makeProduct(null, null))
      else {
        val ok = rightData.filter { r => evalPredicate(p, makeProduct(l, r))}
        if (ok.isEmpty) List(makeProduct(l, null)) else ok.map { r => makeProduct(l, r)}
      }
    }
    }
    printMyOutput(output)
    def value = output
  }



  /** Select Operator
    */
  class SelectOperator(p: Exp, child: ScalaOperator) extends ScalaOperator {

    override def toString() = "select (" + AlgebraPrettyPrinter.pretty(p) + ") " + child
    val output = child.data.filter(evalPredicate(p, _) == true)
    printMyOutput(output)
    def value = output
  }



  class UnnestOperator(path: Exp, p: Exp, child: ScalaOperator) extends ScalaOperator {

    override def toString() = "unnest (" + path + ", " + AlgebraPrettyPrinter.pretty(p) + ") " + child
    private val output = for (v <- child.data; pathV <- expEval(path, v).asInstanceOf[Iterable[_]] if evalPredicate(p, makeProduct(v, pathV))) yield makeProduct(v, pathV)
    printMyOutput(output)
    def value = output
  }

  class OuterUnnestOperator(path: Exp, p: Exp, child: ScalaOperator) extends ScalaOperator {

    override def toString() = "outer-unnest (" + path + ", " + AlgebraPrettyPrinter.pretty(p) + ") " + child

    //private val output = for (v <- child.data; pathV <- expEval(path, v).asInstanceOf[Iterable[Any]]) yield if (evalPredicate(p, makeProduct(v, pathV))) makeProduct(v, pathV) else null
    val output = child.data.flatMap { l => {
      if (l == null) List(makeProduct(null, null))
      else {
        val ok = expEval(path, l).asInstanceOf[Iterable[_]].filter { r => evalPredicate(p, makeProduct(l, r))}
        if (ok.isEmpty) List(makeProduct(l, null)) else ok.map { r => makeProduct(l, r)}
      }
    }
  }
    printMyOutput(output)
    def value() = output
  }

  class NestOperator(m: Monoid, e: Exp, f: ProductCons, p: Exp, g: ProductCons, child: ScalaOperator) extends ScalaOperator {

    override def toString() = "nest (" + List(m, AlgebraPrettyPrinter.pretty(e), f, AlgebraPrettyPrinter.pretty(p), g, child).mkString(", ") + ")"

    // build a ProductCons of the f variables and group rows of the child by its value
    private val valuesG: Map[Any, Iterable[Any]] = child.data.groupBy({l => expEval(f, l)})
    // filter out with predicate p
    private val valuesS: Map[Any, Iterable[Any]] = valuesG.map(kv => (kv._1, kv._2.filter(v => evalPredicate(p, v))))
    // replace the set of rows associated to each "group by value" by the value of the expression e when applied to each row
    // unless the ProductCons(g) evaluates to null, in which case we put the zero.
    private val nullG = BinaryExp(Eq(), g, Null)
    private val valuesE: Map[Any, Iterable[Any]] = valuesS.map(p => (p._1, p._2.map({x: Any => if (expEval(nullG, x) == true) zeroEval(m) else expEval(e, x)})))
    // apply the reduce operation to the set of rows of each group by value
    private val values: Map[Any, Any] = valuesE.map(p => (p._1, p._2.foldLeft(zeroEval(m))({(x: Any, y: Any) => doReduce(m, x, y)})))
    // make a product of the group by value and the reduced operation result
    private val output: List[ProductValue] = values.toList.map{p => makeProduct(p._1, p._2)}
    printMyOutput(output)
    def value() = output
  }

  private def doReduce(m: Monoid, v1: Any, v2: Any): Any = (m, v1, v2) match {
    case (_: ListMonoid, v: Any, l: List[Any]) => List(v) ++ l
    case (_: SetMonoid, v: Any, s: Set[Any]) => Set(v) ++ s
    case (_: AndMonoid, v1: Boolean, v2: Boolean) => v1 && v2
    case (_: OrMonoid, v1: Boolean, v2: Boolean) => v1 && v2
    case (_: SumMonoid, v1: Float, v2: Float) => v1 + v2
    case (_: SumMonoid, v1: Int, v2: Int) => v1 + v2
    case (_: MultiplyMonoid, v1: Float, v2: Float) => v1 * v2
    case (_: MultiplyMonoid, v1: Int, v2: Int) => v1 * v2
    case (_: MaxMonoid, v1: Float, v2: Float) => math.max(v1, v2)
    case (_: MaxMonoid, v1: Int, v2: Int) => math.max(v1, v2)
    case _ => throw RawExecutorRuntimeException(s"cannot reduce($m, $v1, $v2)")
  }

  private def monoidOp(m: Monoid): (Any, Any) => Any = m match {
    case _: ListMonoid => (l, v) => l.asInstanceOf[List[Any]] :+ v
    case _: SetMonoid => (s, v) => s.asInstanceOf[Set[Any]] + v
    case _: AndMonoid => (b1, b2) => b1.asInstanceOf[Boolean] && b2.asInstanceOf[Boolean]
    case _: OrMonoid => (b1, b2) => b1.asInstanceOf[Boolean] || b2.asInstanceOf[Boolean]
    case _: SumMonoid => (v1, v2) => (v1, v2) match {
      case (f1: Float, f2: Float) => f1 + f2
      case (f1: Int, f2: Float) => f1 + f2
      case (f1: Float, f2: Int) => f1 + f2
      case (f1: Int, f2: Int) => f1 + f2
    }
    case _: MultiplyMonoid => (v1, v2) => (v1, v2) match {
      case (f1: Float, f2: Float) => f1 * f2
      case (f1: Int, f2: Float) => f1 * f2
      case (f1: Float, f2: Int) => f1 * f2
      case (f1: Int, f2: Int) => f1 * f2
    }
    case _: MaxMonoid => (v1, v2) => (v1, v2) match {
      case (f1: Float, f2: Float) => math.max(f1, f2)
      case (f1: Int, f2: Float) => math.max(f1, f2)
      case (f1: Float, f2: Int) => math.max(f1, f2)
      case (f1: Int, f2: Int) => math.max(f1, f2)

    }
  }

  /** Reduce Operator
    */
  class ReduceOperator(m: Monoid, e: Exp, p: Exp, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "reduce (" + List(m, AlgebraPrettyPrinter.pretty(e), AlgebraPrettyPrinter.pretty(p), source).mkString(", ") + ")"
    val output = source.data.filter(evalPredicate(p, _)).map(expEval(e, _)).foldLeft(zeroEval(m))(monoidOp(m))
    printMyOutput(output)
    def value() = output
  }

  /** Scan Operator
    */
  class ScanOperator(input: Iterable[Any]) extends ScalaOperator {
    override def toString() = "scan()"
    def value() = input
  }
}
