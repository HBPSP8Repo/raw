package raw

import java.io.File

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.datasets.AccessPath
import raw.executionserver._
import raw.perf.QueryCompilerClient

abstract class AbstractSparkTest(loader: (SparkContext) => List[AccessPath[_ <: Product]])
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with SharedSparkContext
  with ResultConverter
  with LDBDockerContainer {

  var queryCompiler: QueryCompilerClient = _
  var accessPaths: List[AccessPath[_ <: Product]] = _

  def loadTestResult(testName:String):JsonNode = {
    val resultUri = Resources.getResource(s"generated/${testName}.json").toURI
    logger.info("Opening uri: " + resultUri)
    val value: JsonNode = mapper.readTree(new File(resultUri))
    logger.info("Value: " + value)
    value
  }

  def assertJsonEqual(testName:String, queryResult:Any) = {
    val expected = loadTestResult(testName)
    val actual = convertToJsonNode(queryResult)
    logger.info(s"Comparing expected:\n$expected\nActual:\n$actual")
    assert(expected == actual)
  }

  override def beforeAll() {
    super.beforeAll()
    try {
      queryCompiler = new QueryCompilerClient(rawClassLoader)
      accessPaths = loader(sc)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }
}
