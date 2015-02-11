package raw
package executor
package reference

import scala.io.BufferedSource

import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonAST._
import algebra.LogicalAlgebra._
import algebra._

case class ReferenceExecutorError(err: String) extends RawException

class ReferenceResult(result: Any) extends QueryResult {
  val value = result
}

object ReferenceExecutor extends Executor with LazyLogging {

  import Expressions._

  case class ProductValue(items: Seq[Any])

  def makeProduct(x: Any, y: Any): ProductValue =  (x, y) match {
    case (ProductValue(s1), ProductValue(s2)) => ProductValue(s1 ++ s2)
    case (s1: Any, ProductValue(s2)) => ProductValue(Seq(s1) ++ s2)
    case (ProductValue(s1), s2: Any) => ProductValue(s1 :+ s2)
    case (s1: Any, s2: Any) => ProductValue(Seq(s1, s2))
  }

  def execute(root: AlgebraNode, world: World): Either[QueryError, QueryResult] = {

    def dataLocDecode(t: Type, data: Iterable[Any]): DataSource = {
        def recurse(t: Type, value: Any): Any = (t, value) match {
          case (IntType(), v: Int) => v
          case (FloatType(), v: Float) => v
          case (BoolType(), v: Boolean) => v
          case (StringType(), v: String) => v
          case (r: RecordType, v: Map[_, _]) => v match { case x: Map[String, Any] => x.map({ c => (c._1, recurse(r.typeOf(c._1), x(c._1)))}) }
          case (CollectionType(ListMonoid(), what: Type), v: List[_]) => v.map({ x: Any => recurse(what, x)})
          case (CollectionType(SetMonoid(), what: Type), v: Set[_]) => v.map({ x: Any => recurse(what, x)})
          case _ => throw RawExecutorRuntimeException(s"cannot parse $t in $value")
        }
        recurse(t, data) match {
          case l: List[Any] => MemoryDataSource(l)
          case v => throw RawExecutorRuntimeException(s"cannot build a memory scan from $v")
        }
    }

    def loadCSV(t: Type, content: BufferedSource): DataSource = {

      def parse(t: Type, item: String): Any = t match {
        case IntType() => item.toInt
        case FloatType() => item.toFloat
        case BoolType() => item.toBoolean
        case StringType() => item
        case _ => throw RawExecutorRuntimeException(s"cannot parse $t in text/csv files")
      }
      t match {
        case CollectionType(ListMonoid(), RecordType(atts)) => {
          val f = (l: String) => atts.zip(l.split(",")).map{case (a, item) => (a.idn, parse(a.tipe, item))}.toMap
          MemoryDataSource(content.getLines().toList.map(f))
        }
        case _ => throw RawExecutorRuntimeException(s"cannot make data source from $t")
      }
    }

    def loadJSON(t: Type, content: BufferedSource): DataSource = {
      val json: JValue = org.json4s.native.JsonMethods.parse(content.mkString)
      def convert(t: Type, item: JValue): Any = (t, item) match {
        case (IntType(), JInt(i)) => i.toInt
        case (FloatType(), JDouble(d)) => d.toFloat
        case (BoolType(), JBool(b)) => b
        case (StringType(), JString(s)) => s
        case (RecordType(atts), JObject(l)) => {
          val jMap: Map[String, JValue] = l.map{jfield: JField => (jfield._1, jfield._2)}.toMap
          val tMap: Map[String, Type] = atts.map({aType: AttrType => (aType.idn, aType.tipe)}).toMap
          val vMap: Map[String, Any] = jMap.map({j => (j._1, convert(tMap(j._1), j._2))})
          vMap
        }
        case (CollectionType(ListMonoid(), innerType), JArray(arr)) => arr.map({jv => convert(innerType, jv)})
        case (CollectionType(SetMonoid(), innerType), JArray(arr)) => arr.map({jv => convert(innerType, jv)}).toSet // TODO: correct?
      }

      convert(t, json) match {
        case l: List[Any] => MemoryDataSource(l)
        case s: Set[Any] => MemoryDataSource(s.toList) // TODO: correct?
        case v: Any => throw RawExecutorRuntimeException(s"Cannot instantiate a scan from $v")
      }

    }

    def toOperator(opNode: AlgebraNode): ScalaOperator = opNode match {
      case Join(p, left, right) => JoinOperator(p, toOperator(left), toOperator(right))
      case OuterJoin(p, left, right) => OuterJoinOperator(p, toOperator(left), toOperator(right))
      case Select(p, source) => SelectOperator(p, toOperator(source))
      case Nest(m, e, f, p, g, child) => NestOperator(m, e, f, p, g, toOperator(child))
      case OuterUnnest(path, p, child) => OuterUnnestOperator(path, p, toOperator(child))
      case Unnest(path, p, child) => UnnestOperator(path, p, toOperator(child))
      case _: Merge => ???
      case Reduce(m, exp, ps, source) => ReduceOperator(m, exp, ps, toOperator(source))
      case Scan(name) => {
        val source = world.getSource(name)
        source.location match {
          case LocalFileLocation(path, "text/csv") => ScanOperator(loadCSV(source.tipe, scala.io.Source.fromFile(path)))
          case LocalFileLocation(path, "application/json") => ScanOperator(loadJSON(source.tipe, scala.io.Source.fromFile(path)))
          case MemoryLocation(data)              => ScanOperator(dataLocDecode(source.tipe, data))
          case loc                               => throw ReferenceExecutorError(s"Reference executor does not support location $loc")
        }
      }
    }

    logger.debug("\n==========\n" + root + "\n" + LogicalAlgebraPrettyPrinter.pretty(root) + "\n============")
    val operator = toOperator(root)
    operator.next match {
      case r @ Some(v) => Right(new ReferenceResult(v.head))
      case None => throw RawExecutorRuntimeException(s"unexpected None from $root")
    }
  }

  private def evalPredicates(p: FunAbs, items: List[Any]): Boolean = {
    val values = ps.map({p: Exp => expEval(p, items)}).map(toBool)
    values.forall({p: Boolean => p})
  }

  private def toBool(v: Any): Boolean = v match {
    case b: Boolean => b
    case _ => false
  }

  private def expEval(exp: Exp, env: Map[String, Any]): Any = {
    val result = exp match {
      case Null                                         => null
      case BoolConst(v)                                 => v
      case IntConst(v)                                  => v
      case FloatConst(v)                                => v
      case StringConst(v)                               => v
      case IdnExp(IdnUse(v))                            => env(v)
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
    logger.debug("eval " + LogicalAlgebraPrettyPrinter.pretty(exp) + " in " + "(" + env.toString + ") ===> " + result.toString)
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
      case i: Int => i == 0
      case f: Float => f == 0
      case _ => throw RawExecutorRuntimeException(s"cannot compute not($v)")
    }
    case _: Neg => v: Any => v match {
      case i: Int => -i
      case f: Float => -f
      case _ => throw RawExecutorRuntimeException(s"cannot compute neg($v)")
    }
    case _: ToBool => v: Any => v match {
      case _: Boolean => v
      case i: Int => i != 0
      case f: Float => f!= 0
      case _ => throw RawExecutorRuntimeException(s"cannot compute toBool($v)")
    }
    case _: ToInt  => v: Any => v match {
      case true => 1
      case false => 0
      case _: Int => v
      case f: Float => f.toInt
      case _ => throw RawExecutorRuntimeException(s"cannot compute toInt($v)")
    }
    case _: ToFloat => v: Any => v match {
      case true => 1.0f
      case false => 0.0f
      case i: Int => i
      case _: Float => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toFloat($v)")
    }
    case _: ToString => v: Any => v match {
      case b: Boolean => b.toString()
      case i: Int => i.toString()
      case f: Float => f.toString()
      case s: String => v
      case _ => throw RawExecutorRuntimeException(s"cannot compute toString($v)")
    }
  }

  private def binaryOpEval(op: BinaryOperator): (Any, Any) => Any = op match {
    case _: Eq => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 == e2
      case (e1: Int, e2: Float) => e1 == e2
      case (e1: Float, e2: Float) => e1 == e2
      case (e1: Float, e2: Int) => e1 == e2
      case (e1: Boolean, e2: Boolean) => e1 == e2
      case (e1: String, e2: String) => e1 == e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute eq($i1, $i2)")
    }
    case _: Neq => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 != e2
      case (e1: Int, e2: Float) => e1 != e2
      case (e1: Float, e2: Float) => e1 != e2
      case (e1: Float, e2: Int) => e1 != e2
      case (e1: Boolean, e2: Boolean) => e1 != e2
      case (e1: String, e2: String) => e1 != e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute neq($i1, $i2)")
    }
    case _: Lt => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 < e2
      case (e1: Int, e2: Float) => e1 < e2
      case (e1: Float, e2: Float) => e1 < e2
      case (e1: Float, e2: Int) => e1 < e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute lt($i1, $i2)")
    }
    case _: Le => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 <= e2
      case (e1: Int, e2: Float) => e1 <= e2
      case (e1: Float, e2: Float) => e1 <= e2
      case (e1: Float, e2: Int) => e1 <= e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute le($i1, $i2)")
    }
    case _: Ge => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 >= e2
      case (e1: Int, e2: Float) => e1 >= e2
      case (e1: Float, e2: Float) => e1 >= e2
      case (e1: Float, e2: Int) => e1 >= e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute ge($i1, $i2)")
    }
    case _: Gt => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 > e2
      case (e1: Int, e2: Float) => e1 > e2
      case (e1: Float, e2: Float) => e1 > e2
      case (e1: Float, e2: Int) => e1 > e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute gt($i1, $i2)")
    }
    case _: Div => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 / e2
      case (e1: Int, e2: Float) => e1 / e2
      case (e1: Float, e2: Float) => e1 / e2
      case (e1: Float, e2: Int) => e1 / e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute div($i1, $i2)")
    }
    case _: Sub => (i1: Any, i2: Any) => (i1, i2) match {
      case (e1: Int, e2: Int) => e1 - e2
      case (e1: Int, e2: Float) => e1 - e2
      case (e1: Float, e2: Float) => e1 - e2
      case (e1: Float, e2: Int) => e1 - e2
      case _ => throw RawExecutorRuntimeException(s"cannot compute sub($i1, $i2)")
    }
  }

  abstract class ScalaOperator {
    def next: Option[Any]

    case class OperatorBufferedOutput(listValue: List[Any]) {
      private var index: Int = 0
      private val L: List[Any] = listValue

      def next(): Option[Any] = {
        val n = index
        index += 1
        val nextValue = {
          try {
            Some(L(n))
          } catch {
            case ex: IndexOutOfBoundsException => None
          }
        }
        nextValue
      }
    }
  }

  /** Join Operator
    */
  // TODO: make non-case classes
  case class JoinOperator(p: FunAbs, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "join (" + ExpressionPrettyPrinter.pretty(p).mkString(" && ") + ") " + left + " X " + right

    private def readRight: List[Any] = right.next match {
      case Some(v) => List(v) ++ readRight
      case None => List()
    }

    private val rightValues: List[Any] = readRight

    def recurse: List[ProductValue] = left.next match {
      case r@Some(v: Any) => (rightValues.filter{l: Any => expEval(p.e, Map(p.idn.idn -> makeProduct(v, l))).asInstanceOf[Boolean]}).map{l: Any => makeProduct(v, l)} ++ recurse
      case None => List()
    }

    private val matches = OperatorBufferedOutput(recurse)
    def next = matches.next()
  }


  case class OuterJoinOperator(p: FunAbs, left: ScalaOperator, right: ScalaOperator) extends ScalaOperator {

    override def toString() = "outer-join (" + ExpressionPrettyPrinter.pretty(p).mkString(" && ") + ") " + left + " X " + right

    private def readRight: List[List[Any]] = right.next match {
      case Some(v: List[Any]) => List(v) ++ readRight
      case None => List()
    }

    private val rightValues: List[List[Any]] = readRight

    def recurse: List[List[Any]] = left.next match {
      case r@Some(v: List[Any]) => {
        val matches = rightValues.filter({l: List[Any] => evalPredicates(ps, v ++ l)})
        if (matches == List())
          List(v ++ List(null)) ++ recurse
        else matches.map({i: List[Any] => v ++ i}) ++ recurse
      }
      case None => List()
    }

    private val matches = OperatorBufferedOutput(recurse)
    def next = matches.next()

  }



  /** Select Operator
    */
  case class SelectOperator(p: FunAbs, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "select (" + ExpressionPrettyPrinter.pretty(p).mkString(" && ") + ") " + source

    private val child = source
    def recurse: Option[List[Any]] = child.next match {
      case r@Some(v) => if(evalPredicates(ps, v)) r else recurse
      case None => None
    }

    def next = recurse
  }



  case class UnnestOperator(p: Path, p: FunAbs, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "unnest (" + p + ", " + ExpressionPrettyPrinter.pretty(p).mkString(" && ") + ") " + source

    private val child = source

    def extract(p: Path, v: List[Any]): Any = (p, v) match {
      case (_, List(null)) => null
      case (BoundArg(a), _) => expEval(a, v)
      case (InnerPath(p2, name), _) => extract(p2, v) match {
        case m: Map[String, Any] => m(name)
        case err => throw RawExecutorRuntimeException(s"cannot extract inner path from $err")
      }
      case _ => throw RawExecutorRuntimeException(s"path $p of $v is not supported")
    }

    def recurse: List[List[Any]] = child.next match {
      case r@Some(v: List[Any]) => {
        val pathValue = extract(p, v)
        // it should be Null or a Collection of values
        pathValue match {
          case s: Set[Any] => s.toList.map({i: Any => v :+ i}).filter({tuple => evalPredicates(ps, tuple)}) ++ recurse
          case l: List[Any] => l.map({ i: Any => v :+ i}).filter({tuple => evalPredicates(ps, tuple)}) ++ recurse
          case null => recurse
          case _ => throw RawExecutorRuntimeException(s"unexpected $pathValue in unnest")
        }
      }
      case None => List()
    }

    val values = OperatorBufferedOutput(recurse)
    def next = values.next()
  }

  case class OuterUnnestOperator(p: Path, p: FunAbs, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "outer-unnest (" + p + ", " + ExpressionPrettyPrinter.pretty(p).mkString(" && ") + ") " + source

    private val child = source

    def extract(p: Path, v: List[Any]): Any = (p, v) match {
      case (_, List(null)) => null
      case (BoundArg(a), _) => expEval(a, v)
      case (InnerPath(p2, name), _) => extract(p2, v) match {
        case m: Map[String, Any] => m(name)
        case err => throw RawExecutorRuntimeException(s"cannot extract inner path from $err")
      }
      case _ => throw RawExecutorRuntimeException(s"path $p of $v is not supported")
    }

    def recurse: Option[List[Any]] = child.next match {
      case r@Some(v) => {
        val pathValue = extract(p, v)
        if(evalPredicates(ps, v)) {
          Some(v ++ List(pathValue))
        } else recurse
      }
      case None => None
    }

    def next = recurse
  }

  case class NestOperator(m: Monoid, e: Exp, f: FunAbs, p: FunAbs, g: FunAbs, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "nest (" + List(m, ExpressionPrettyPrinter.pretty(e), f, ExpressionPrettyPrinter.pretty(p), g, source).mkString(", ") + ")"

    private def readSource: List[List[Any]] = source.next match {
      case Some(v: List[Any]) => List(v) ++ readSource
      case None => List()
    }

    // map from groupBy variables to list of elements
    // the groupBy value is the list of arguments in "f" evaluated against each value read from the source
    private val valuesS: Map[List[Any], List[List[Any]]] = readSource.groupBy({l => f.map({v => expEval(v, l)})})
    // list of elements should be replaced by the list of values of e if g doesn't show null values
    //private val valuesE: Map[List[Any], List[Any]] = valuesS.map(p => (p._1, p._2.map({_ => expEval(e, _)})))

    // returns true if any of the variables in 'g' evaluates to NullValue.
    def skip(env: List[Any]): Boolean = {
      g.map{v: Arg => expEval(v, env)}.exists({x: Any => x == null})
    }

    private val valuesE: Map[List[Any], List[Any]] = valuesS.map(p => (p._1, p._2.map({x: List[Any] => if (skip(x)) zeroEval(m) else expEval(e, x)})))
    // list of values of e should be reduced according to the monoid
    private val values = valuesE.map(p => (p._1, p._2.foldLeft(zeroEval(m))({(x: Any, y: Any) => doReduce(m, x, y)})))
    // make the list of list
    //private val listOfList: List[List[Any]] = values.toList.map{p => List(p._1, p._2)}
    private val listOfList: List[List[Any]] = values.toList.map{p => p._1 ++ List(p._2)}

    private val matches = OperatorBufferedOutput(listOfList)
    def next = matches.next()

  }

  private def doReduce(m: Monoid, v1: Any, v2: Any): Any = (m, v1, v2) match {
    case (_: ListMonoid, v: Any, l: List[Any]) => List(v) ++ l
    case (_: SetMonoid, v: Any, s: Set[Any]) => Set(v) ++ s
    case (_: AndMonoid, v1: Boolean, v2: Boolean) => v1 && v2
    case (_: OrMonoid, v1: Boolean, v2: Boolean) => v1 && v2
    case (_: SumMonoid, v1: Int, v2: Int) => v1 + v2
    case (_: MultiplyMonoid, v1: Int, v2: Int) => v1 * v2
    case (_: MaxMonoid, v1: Int, v2: Int) => math.max(v1, v2)
    case _ => throw RawExecutorRuntimeException(s"cannot reduce($m, $v1, $v2)")
  }

  /** Reduce Operator
    */
  case class ReduceOperator(m: Monoid, e: FunAbs, p: FunAbs, source: ScalaOperator) extends ScalaOperator {

    override def toString() = "reduce (" + List(m, ExpressionPrettyPrinter.pretty(e), ExpressionPrettyPrinter.pretty(p), source).mkString(", ") + ")"

    def recurse: Any = source.next match {
      case None => zeroEval(m)
      case r@Some(v) => doReduce(m, expEval(e.e, Map(e.idn.idn -> v)), recurse)
    }

    val value = OperatorBufferedOutput(recurse)
    def next() = value.next()
  }

  /** Scan Operator
    */
  case class ScanOperator(d: DataSource) extends ScalaOperator {
    override def toString() = "scan()"
    private val source = d
    def next = d.next()
  }
}
