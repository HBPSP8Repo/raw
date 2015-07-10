package raw.patients.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.datasets.patients._
import raw.patients._


@rawQueryAnnotation
class Count0Query(val patients: RDD[Patient]) extends RawQuery {
  val oql = """
    count(patients)
  """
}


class CountTest extends AbstractSparkPatientsTest {

  test("Count0") {
    val result = new Count0Query(patientsRDD).computeResult
    val actual = convertToString(result)
    val expected = "500"
    assert(actual === expected, s"\nActual: $actual\nExpected: $expected")
  }

}
