//package raw.csv.spark
//
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.spark.rdd.RDD
//import raw.Raw
//import shapeless.HList
//
//import scala.reflect.runtime.{universe => ru}
//
//object SparkBasicTest extends StrictLogging {
//  var primesInt: RDD[Int] = null
//  var primesIntScala: List[Int] = null
//  var primesStrings: RDD[String] = null
//  var primesStringsScala: List[String] = null
//  var primesInteger: RDD[Integer] = null
//
//  /* TODO: The code generated for this query does not type check when the input is a list of value types.
//    The case class created to hold the result takes two Ints as arguments but by the time the generated
//    code tries to create instances of this class, the type inferred by the compiler is Any, which is not
//    compatible with Int.
//   */
//  //  def primesNest(): Unit = {
//  //    val res = Raw.query(
//  //      """for (s1 <- primes) yield set (
//  //        prime := s1,
//  //        count := (for (s2 <- primes) yield sum 1))""",
//  //      HList("primes" -> primesIntScala))
//  //    logger.info("RES: {}\n", res)
//  //  }
//
//  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
//
//  def primesNestRefs(): Unit = {
//    val res = Raw.query(
//      """for (s1 <- primes) yield set (
//        prime := s1,
//        count := (for (s2 <- primes) yield sum 1))""",
//      HList("primes" -> primesStringsScala))
//    //      HList("primes" -> primesStrings))
//
//    val tag = getTypeTag(res)
//    logger.info("res: {} : {} : {}", res, res.getClass, tag)
//    val asTuples = res.map(v => (v.prime, v.count))
//    logger.info("RES: {}\n", asTuples)
//  }
//}
//
//class SparkBasicTest extends AbstractSparkFlatCSVTest {
//  SparkBasicTest.primesInteger  = rawSparkContext.sc.makeRDD(List[Integer](2, 3, 5, 7, 11, 13, 17))
//  SparkBasicTest.primesStrings = rawSparkContext.sc.makeRDD(List("2", "3", "5", "7", "11", "13", "17"))
//  SparkBasicTest.primesInt = rawSparkContext.sc.makeRDD(List(2, 3, 5, 7, 11, 13, 17))
//  SparkBasicTest.primesStringsScala = List("2", "3", "5", "7", "11", "13", "17")
//  SparkBasicTest.primesIntScala = List(2, 3, 5, 7, 11, 13, 17)
//
//  test("unnester with value types") {
//    SparkBasicTest.primesNestRefs()
//  }
//
//}
