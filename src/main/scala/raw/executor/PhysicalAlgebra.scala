//package raw.executor
//
//import scala.reflect.runtime.{universe => ru}
//import raw.algebra.LogicalAlgebra._
//import raw.algebra.Expressions._
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//
//object PhysicalAlgebra {
//
//  sealed abstract class PhysicalAlgebraNode extends raw.RawNode
//
//  case class Scan(input: Any) extends PhysicalAlgebraNode
//
//  case class Map(e: Exp, child: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class Filter(p: Exp, child: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class Fold(m: raw.Monoid, child: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class Join(left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class LeftOuterJoin(left: PhysicalAlgebraNode, right: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class Unnest(e: Exp, child: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//
//  case class OuterUnnest(e: Exp, child: PhysicalAlgebraNode) extends PhysicalAlgebraNode
//}
//
//
//object ToPhysicalAlgebra {
//
//  def apply(a: LogicalAlgebraNode): PhysicalAlgebra.PhysicalAlgebraNode = a match {
//    case Scan(input) =>
//      PhysicalAlgebra.Scan(input)
////    case Scan(???) =>
////      PhysicalAlgebra.Scan(input)
//    case Select(p, child) =>
//      PhysicalAlgebra.Filter(p, apply(child))
//    case Unnest(path, pred, child) =>
//      PhysicalAlgebra.Filter(pred, PhysicalAlgebra.Unnest(path, apply(child)))
//    case OuterUnnest(path, pred, child) =>
//      PhysicalAlgebra.Filter(pred, PhysicalAlgebra.OuterUnnest(path, apply(child)))
//    case Reduce(m, e, p, child) =>
//      PhysicalAlgebra.Fold(m, PhysicalAlgebra.Map(e, PhysicalAlgebra.Filter(p, apply(child))))
//    case Join(p, left, right) =>
//      PhysicalAlgebra.Filter(p, PhysicalAlgebra.Join(apply(left), apply(right)))
//    case OuterJoin(p, left, right) =>
//      PhysicalAlgebra.Filter(p, PhysicalAlgebra.LeftOuterJoin(apply(left), apply(right)))
//  }
//
//}
//
//object PhysicalOperators {
//
//  sealed abstract class PhysicalOperatorNode extends raw.RawNode
//
//  case class ScalaScan(input: Any) extends PhysicalOperatorNode
//
//  case class SparkScan(input: RDD[Any]) extends PhysicalOperatorNode
//
//  case class ScalaMap extends PhysicalOperatorNode
//
//  case class SparkMap extends PhysicalOperatorNode
//
//}
//
//object ToPhysicalOperators {
//
//  def apply(a: PhysicalAlgebra.PhysicalAlgebraNode): PhysicalOperators.PhysicalOperatorNode = a match {
//    // build bottom up and make scala/spark joins combined with ScalaToSpark...
//  check signature in OUR catalog obtained via reflection
//  and build a ScanSpark or a ScanScala as appropriate
//  }
//}
//
//object ExpressionBuilder {
//  def apply(e: Exp): ...
//}
//
//object Executor {
//
//  def apply(op: PhysicalOperators.PhysicalOperatorNode) = op match {
//
//  }
//
//}
//
//
////add support for user functions
////split into functions
////and into objects that one reads???
////
////
////one option: i just get their names :)
////but i need access to their scope, right?
////i declare them as a list of anys?
////maybe that works
////so I say:
////Map('foo' -> foo)
////and the signature is Map[String, Any]
////then, here, in the World - actually, in the query -, I call reflection on any to know what its signature actually is.
////I check that it is e.g. of type Iterable[Any] or whatever.
////if so, that is fine.
////But when I use it... I...
////well, in the type checker I must know its type and convert the scala type to types in our world.
////humm.
////
////so I guess the first step is to try to do that sort of reflection.. separately.. and see how it goes!
//
//
//object TestPhysical extends App {
//
////  import algebra.LogicalAlgebra._
////
////  val t =
////    PhysicalAlgebra.Map(
////      ???,
////      PhysicalAlgebra.Scan())
//
//  println("Hello world")
//
//  def iterator: List[String] = List("aa", "bb", "cc")
//
//  println(iterator)
//
//  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
//
//  def getTypeOf[T](x: T)(implicit tag: ru.TypeTag[T]) = ru.typeOf[T]
////
//// def getRawType[A](x: Any): Type =
////    if (x == ru.typeOf[List[A]]) ListType(getRawType(A))
//
//def iterator2: Set[List[String]] = Set(List("1", "a"), List("2", "b"))
//
//
////  def getRawType[A](x: A)(implicit tag: ru.TypeTag[A]): Type = x match {
////    case _: Int => IntType()
////    case _: List => ru.typeOf[A].typeArgs.head
//////    val targs = ru.typeOf[A] match {
//////      case ru.TypeRef(_, _, args) => args
//////    }
//////    targs
////  }
////    //ru.typeOf[A]  .members.filter(!_.isMethod).foreach(sym => println(sym + " is a " + sym.typeSignature))
//
//  println(getTypeTag(iterator).tpe == ru.typeOf[List[String]])
//
////  println(getRawType(iterator))
//
////  println(getRawType(iterator2))
//
//  println(getTypeOf(iterator))
//  println(getTypeOf(iterator2))
//
//  import scala.reflect.api.Types
//
//  import ru._
//
//  def getRawType[T](x: T)(implicit tag: TypeTag[T]): raw.Type = {
//    def recurse(t: Type): raw.Type = t match {
//        // Add support for any :)
//        // Add support for case classes becoming record types
//        // What about maps??? what do those become? RecordType of type variable...
//      case TypeRef(_, c, Nil) if c.fullName == "scala.Int" => raw.IntType()
//      case TypeRef(_, c, List(t1)) if c.fullName == "scala.Predef.Set" => raw.SetType(recurse(t1))
//      case TypeRef(_, c, List(t1)) if c.fullName == "scala.List" => raw.ListType(recurse(t1))
//      case TypeRef(_, c, List()) if c.fullName == "scala.Predef.String" => raw.StringType()
//      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Tuple") =>
//        val regex = """scala\.Tuple(\d+)""".r
//        c.fullName match {
//          case regex(n) => raw.RecordType(List.tabulate(n.toInt){ case i => raw.AttrType(s"_${i + 1}", recurse(t1(i))) })
//        }
//      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Function") =>
//        val regex = """scala\.Function(\d+)""".r
//        c.fullName match {
//          case regex(n) => raw.FunType(recurse(t1(0)), recurse(t1(1)))
//        }
//      case TypeRef(a, b, c) => println(s"a $a b ${b.fullName} c $c"); ???
//    }
//
//    recurse(getTypeOf(x))
//  }
//
//  println(getRawType(iterator2))
//
//  def foo: (String, Int) = ("a", 2)
//  println(getRawType(foo))
//
//  def foo2(a: Int): List[(String, Int)] = List(("x", a), ("y", a + 2))
//  println(s"foo2 is ${getRawType(foo2 _)}")
//
//  //println(getRawType({x: Int => foo2(x)}))
////  var x = scala.collection.mutable.List()
////  x += foo
////  println(getRawType(x(0)))
//
//  val nfoo = getRawType(foo)
//  println(s"nfoo is $nfoo")
//
////val x = "def (x: Any): RDD[Something Meaningful] = map(...}.filter.toRDD[...]"
////  x(input)
////
////
////  """
////     for (d <- Departments) yield set {
////       name := googlesuggest(d.name)
////      (fixedName := name, cucu := name + " blah ")
////     }
////  """
////
////  1) serialize googlesuggest and ship it down as a "fun abs". The most elegant way.
////  2)
////
////  // USE CASE 1: Give user access to "our" RDD object so that (s)he can write a function that reads data from the cluster.
////
////  def readInput(sc: SparkContext, path: String, businessLogic: Int): RDD[(Int, String)] =
////    sc.textFile(path).map{ case line => val tuple = line.split(","); (tuple._1.toInt * businessLogic, tuple._2) }
////
////  val raw = Raw.newContext()  // Raw is a singleton object; it gives a context per query (?)
////  raw.registerScala("input", readInput)
////
////  raw.query("""for ((a, b) <- input("/foo.txt", 5), a > 10) yield set b""")
////
////  // Function 'readInput' must be serialized.
////
////  // USE CASE 2: User wants to call a Scala function or use a Scala value to create the output.
////
////  def doSomethingSmart(x: Int) = x * 42
////
////  val raw = Raw.newContext()
////  raw.registerScala("smart", doSomethingSmart)
////  raw.registerSparkCSV("input", "/numbers.txt", ",") // This is sugar for a SparkCSV parser!
////
////  raw.query("""for ((a,b,c) <- input, a > 5) yield set smart(b + c)""")
////
////  //to keep in mind: the optimizer should be global, hence the sparkcontext should be shared...
//
//  val conf = new SparkConf().setAppName("RAW Spark").setMaster("local")
//  val sc = new SparkContext(conf)
//
////  //sc.textFile("/foo.txt").map
////
////  q: why do i need to serialize the function? to put it in the context?
////  yes, because I use it in an expression, which would be quasiquote-generated...
////
////
////  so that is the problem...
////  ...if it is NOT in an expression,
////  ...then it is a collection
////  ...and hence, it is a SCAN node.
////
////  so there are actually a few possibilities for handling user collections:
////
////    one is to have a single sparkcontext, give it directly to the user, and do single-query optimization first.
////    in this case, there is no issue with serializing the spark context. The Scan node will include a reference to the
////    user object, whatever that may be.
////    single-query optimization may still get better if we keep track of cardinalities et al on the raw side; but there
////    is no sharing of actual data caches.
////
////    the other is that the user himself creates the sparkcontext and passes it to raw instead.
////    well, the user doesnt even have to pass it to raw; raw just recognizes the signature type and if it is an RDD, it
////    generated RDD-friendly operator code.
////    but i think raw needs the sc object to be able to turn plain Scala lists et al into RDDs when doing a join!
////    so this option ain't great...
////
////    yet another option is to have a separate spark proxy. Then whenever we build a query, we have to turn the user code
////    into a scala file, compile it to a jar and ship it for execution. There's quite a bit of tooling here, for input/
////    output. The user code also needs to be "serialized" to be included in quasiquotes. The solution is not very REPL-
////    -friendly. The advantage is that we have - theoretically - cache-reuse for the optimizer.
////
////  as for expressions, what can we do?
////  one option is to turn the quey into a DAG, and then the user input is either a collection, or simply a constant.
////
////  for (...) yield set (a := googlesuggest(foo))
////
////  DAG: find all "parents" of the expression 'googlesuggest'
////
////  inside raw: for (...) yield set BLAH
////  outside raw: googlesuggest(BLAH)
////  inside raw: for (x <- BLAH) yield set (a := )
////
////
////
////  def foo: RDD[(Int, String)]
////
////  raw.register("foo", foo)
////
////  def registerCollection[T <: Iterable](name: String, obj: T) =
////    get raw type via reflection
////    store obj as List/Set/whatever[Any] and store also the raw type and the fact it is a a scala collection
////
////  def registerSpark[T <: RDD[_]](name: String, obj: T) =
////    get raw type via reflection
////    store obj as RDD[Any] and store also the raw type and the fact it is a a spark collection
////
////
////  then, in the expression generator, I do inside .asInstanceOf[...] because we saw that's not actually so slow anymore.
////   quasiquoted stuff: "map { case v => v.asInstanceOf[...].{whatever}}"
////
////
////what is missing: scala calls as output...
////`
//
//
//  import scala.reflect.runtime.{universe => ru}
//  import scala.reflect.runtime.universe._
//
//  import scala.tools.reflect.ToolBox
//
//  protected val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
//
//  val textCode = s"""
//    (f: (String => String)) => {
//      x => f(x)
//    }
//    """
//  println(textCode)
//  val code = toolbox.parse(textCode)
//  val g = toolbox.eval(code).asInstanceOf[Any => (Any => Any)]
//  def googleSearch(x: String): String = x.toUpperCase
//
//  val x = g(googleSearch _)
//  println(x("ben"))
//
//  sc.textFile("C:\\Users\\Miguel Branco\\Documents\\foo.txt").mapPartitions{ case p => p.map { case v => x(v) }}.foreach(println)
//
//}