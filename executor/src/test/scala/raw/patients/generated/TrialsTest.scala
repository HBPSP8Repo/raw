// TODO: Fails because of inconsistent ordering of fields in logical algebra.
//package raw.patients.generated
//import org.apache.spark.rdd.RDD
//import raw.{rawQueryAnnotation, RawQuery}
//import raw.datasets.patients._
//import raw.patients._
//
//
//@rawQueryAnnotation
//class Trials0Query(val patients: RDD[Patient]) extends RawQuery {
//  val oql = """
//    select G, count(partition) from patients P group by G: struct(a:P.city, b:P.country)
//  """
//}
//
////@rawQueryAnnotation
////class Trials1Query(val patients: RDD[Patient]) extends RawQuery {
////  val oql = """
////    select distinct I.gender, (select distinct country, count(partition) from I.people P group by country:P.country) as G from (
////    select distinct gender, (select P from partition) as people from patients P group by gender: P.gender
////    ) I
////  """
////}
//
//@rawQueryAnnotation
//class Trials2Query(val patients: RDD[Patient]) extends RawQuery {
//  val oql = """
//    select distinct T.gender, (select city, (select distinct patient_id from partition) as c from T.people P group by city: P.city) as X from
//    (select distinct gender, (select p from partition) as people from patients p group by gender: p.gender) T
//  """
//}
//
//
//class TrialsTest extends AbstractSparkPatientsTest {
//
//  test("Trials0") {
//    val result = new Trials0Query(patientsRDD).computeResult
//    val actual = convertToString(result)
//
//    println(actual)
//  }
//
////  test("Trials1") {
////    val result = new Trials1Query(patientsRDD).computeResult
////    val actual = convertToString(result)
////
////    println(actual)
////  }
//
//  test("Trials2") {
//    val result = new Trials2Query(patientsRDD).computeResult
//    val actual = convertToString(result)
//
//    println(actual)
//  }
//
//}
