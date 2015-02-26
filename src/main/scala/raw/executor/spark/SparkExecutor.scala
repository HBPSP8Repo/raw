//package raw
//package executor
//package spark
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import raw.algebra.PhysicalAlgebra
//
////import scala.language.experimental.macros
////import org.apache.spark.rdd.RDD
//
//case class SparkExecutorError(err: String) extends RawException
//
//class SparkResult(result: Any) extends QueryResult {
//  val value = result
//}
//
////import scala.reflect.runtime.universe._
////import scala.reflect.runtime.currentMirror
////import scala.tools.reflect.ToolBox
//
//
//object SparkExecutor extends Executor {
//
//  // This Works:
//  //  import scala.reflect.runtime.universe._
//  //  import scala.reflect.runtime.currentMirror
//  //  import scala.tools.reflect.ToolBox
//  //  val toolbox = currentMirror.mkToolBox()
//
//
//  //  import scala.reflect.runtime.{universe => ru}
////  import scala.reflect.runtime.universe._
////  val toolbox = ru.runtimeMirror(getClass.getClassLoader).mkToolBox()
////  import scala.tools.reflect.ToolBox
//
//
//
////  import scala.reflect.runtime.{universe => ru}
////  import scala.reflect.runtime.universe._
////  val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
////  val cl = getClass.getClassLoader.asInstanceOf[java.net.URLClassLoader]
////  val cp = cl.getURLs().map(_.getFile()).map(_.stripPrefix("/C:")).map(_.replace("%20", " ")).mkString(System.getProperty("path.separator"))
////  val toolbox = ru.runtimeMirror(getClass.getClassLoader).mkToolBox(options = s"""-cp $cp""")
//
//  import algebra.PhysicalAlgebra._
//
//  //@transient
//  val conf = new SparkConf().setAppName("RAW Spark").setMaster("local")
//
//  //@transient
//  val sc = new SparkContext(conf)
//
//  import org.kiama.util.Counter
//
//  //@transient
//  val counter = new Counter(0)
//
//  //       SparkExecutor is likely not an object because it needs to keep state.
//  //       Should an executor return a new world? Or a world diff?
//
//  def freshName(): String = {
//    val n = counter.value
//    counter.next()
//    s"var$n"
//  }
//
//  def execute(root: OperatorNode, world: World): Either[QueryError, QueryResult] = {
//
//    def buildTuple(atts: Seq[AttrType], line: String): Map[String, Any] = {
//      def parse(t: raw.Type, item: String): Any = t match {
//        case IntType()    => item.toInt
//        case FloatType()  => item.toFloat
//        case BoolType()   => item.toBoolean
//        case StringType() => item
//        case _            => throw new RuntimeException(s"Type $t not supported in text/csv files")
//      }
//      atts.zip(line.split(",")).map{ case (att: AttrType, col: String) => (att.idn, parse(att.tipe, col)) }.toMap
//    }
//
//
//
//    //    def buildZeroMonoid(m: Monoid): String = {
////      val code = m match {
////        case _: SumMonoid => "0"
////      }
////      val compiledCode = toolbox.compile(code)
////      val zero: Int = compiledCode()
////      zero
////    }\
////
////    def foo(s: Seq[String]) = s(0)
////
////
////
////
////    case class Doo(i: Int)
////
////    implicit val liftDoo = Liftable[Doo] { p => q"Doo(${p.i})"
////      //q"_root_.points.Point(${p.x}, ${p.y})"
////    }
////    implicit val liftSeqDoo = Liftable[Seq[Doo]] { p => q"Seq(..$p)" //q"Seq(${p mkString (",")})"  //"Seq(..$p)"
////      //q"_root_.points.Point(${p.x}, ${p.y})"
////    }
////
////    //implicit val liftIntType = Liftable[IntType] { p => q"IntType()" }
//
////    implicit val liftType = Liftable[raw.Type] {
////      p => p match {
////        case IntType() => q"IntType()"
////        case FloatType() => q"FloatType()"
////        case BoolType() => q"BootType()"
////        case StringType() => q"StringType()"
////        case t => throw RawExecutorRuntimeException(s"Type $t not supported in text/csv files")
////      }
////    }
////
////    implicit val liftAttrType = Liftable[AttrType] { p => q"AttrType(${p.idn}, ${p.tipe})" }
////
////    case class Code(code: Seq[Tree], varName: TermName)
//
////    def buildPredicate(ps: List[algebra.Exp]): (Any => Boolean) = {
////      val code = q"""((p: Any) => false)"""
////      val compiledCode = toolbox.compile(code)().asInstanceOf[(Any => Boolean)]
////      compiledCode
////    }
//
////    def buildPredicate(ps: List[algebra.Exp]): (Any => Boolean) = {
////      val code = q"""true"""
////      val compiledCode = toolbox.compile(code)().asInstanceOf[Boolean]
////      (p => compiledCode)
////    }
//
//
//
//    def buildSparkCode(n: OperatorNode): Any = {
//      n match {
//        case Scan(CollectionType(_: ListMonoid, RecordType(atts)), LocalFileLocation(path, "text/csv")) =>
//          sc.textFile(path).mapPartitions{ iter => iter.map(Codegen.buildTuple(atts)) }
//          //sc.textFile(path).map(buildTuple(atts, _))
//        case algebra.PhysicalAlgebra.Select(ps, child) => {
//          val childPlan = buildSparkCode(child)
////          val x = childPlan.asInstanceOf[RDD[Any]].filter(buildPredicate(ps))
//          val x = childPlan.asInstanceOf[RDD[Any]].filter(Codegen.buildPredicate(ps))
//          println(x.collect())
//          Thread.sleep(10000)
//          x
//        }
//        case Reduce(_: SumMonoid, e, ps, child) => {
//          val childPlan = buildSparkCode(child)
//          ///childPlan.asInstanceOf[RDD[Any]].aggregate(0)((acc, n) => acc + buildExpr(e)(n), (a: Int, b: Int) => a + b)
//          //val t1: Any = childPlan.asInstanceOf[RDD[Any]].filter(Codegen.buildPredicate(ps))
//          val t1: Any = childPlan.asInstanceOf[RDD[Any]].mapPartitions{ iter => iter.filter(Codegen.buildPredicate(ps)) }
//
//
//          //val t2: RDD[Int] = t1.asInstanceOf[RDD[Any]].mapPartitions{ iter => iter.map(Codegen.one()) }
//          val t2 = t1.asInstanceOf[RDD[Any]].mapPartitions{ iter => iter.map(Codegen.one()) }
//          val t3 = t2.mapPartitions{ iter => iter.map(Set(_)) }
//
//          val x = Set[Any]()
//          val y = x.+(1).+(2)
//          val z = x.+(2)
//          val w = x ++ y
//          println(w)
//
//          //t2.aggregate(0)((acc, n) => acc + n, (a, b) => a + b)
//          //t2.reduce((a, b) => a + b)
//
//          t3.reduce((a, b) => a ++ b)
//
//          t1.asInstanceOf[RDD[Any]].aggregate(Set[Any]())((acc, n) => acc.+(n), (a, b) => a ++ b)
//
//          //t1.asInstanceOf[RDD[Map[String, Any]]].reduce((a, b) => a ++ b)
//
//          //t1.asInstanceOf[RDD[Any]].aggregate(0)(Codegen.hack(), (a, b) => a + b)
//          //t1.asInstanceOf[RDD[Any]].aggregate(0)((acc, n) => acc + 1, (a, b) => a + b)
//
//          //childPlan.asInstanceOf[RDD[Any]].aggregate(0)((acc, n) => acc + 1, (a, b) => a + b)
//        }
//        case loc =>
//          throw SparkExecutorError(s"Spark executor does not support location $loc")
//      }
//    }
//
//    val code = buildSparkCode(root)
//    Right(new SparkResult(code))
//  }
//}
//
///*
//
//        }
//
//          //        import universe._
//          //        val x = 1
//          //        val y = 3
//          //
//          //        buildSparkPlan(child).filter(tuple => {
//          //          val code = q"$tuple > $y"
//          //          val compiledCode = toolbox.compile(code)
//          //          val func = compiledCode()
//          //          func.asInstanceOf[Boolean]
//          //        })
////        }
////        case Reduce(m, e, ps, child) => {
////
////
////
////        }
////
////          m match {
////            case _: Su
////          }
////          buildSparkPlan(child).filter(buildPredFunc(ps)).foldLeft(buildZeroMonoid(m))((a, b) => buildConsMonoid(a, b))
// */