package raw

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.spark._
import raw.executor.{RawCompiler, ResultConverter, RawClassLoader, RawCompiler$}

import scala.collection.JavaConversions

abstract class AbstractRawTest
  extends FunSuite
  with StrictLogging
  with BeforeAndAfterAll
  with ResultConverter
  with RawClassLoader {

  var queryCompiler: RawCompiler = _

  override def beforeAll() {
    super.beforeAll()
    try {
      queryCompiler = new RawCompiler(rawClassLoader)
    } catch {
      case ex: Exception =>
        super.afterAll()
        throw ex
    }
  }

  /*
   * Writes the json tree in a canonical form, suitable for comparison.
   * JSON objects are written sorted by keys and Json ARRAYs sorted by the string representing
   * each entry.
   * NOTE: does not respect order of arrays. This means that two trees which differ only in the
   * order by which arrays store their elements will be considered equal
   *
   * This is a workaround until we decide on a representation to return the results of a query
   * from the REST server that makes a distinction between sets, lists and bags. This is not
   * possible using Json.
   */
  def toStringOrdered(node: JsonNode): String = {
    node.getNodeType() match {
      case JsonNodeType.ARRAY =>
        val elems = JavaConversions.asScalaIterator(node.elements()).toList
        logger.trace("Array with elements: $elems")
        val strings = elems.map(jn => toStringOrdered(jn))
        strings.sorted.mkString("[", ",", "]")
      case JsonNodeType.OBJECT => {
        val fieldNames: List[String] = JavaConversions.asScalaIterator(node.fieldNames()).toList.sorted
        logger.trace("Object with fields: $fieldNames")
        val strings = fieldNames.map(name => {
          s""""$name":${toStringOrdered(node.get(name))}"""
        })
        strings.mkString("{", ", ", "}")
      }
      case JsonNodeType.STRING =>
        // Quote the special characters " and \ so we can reparse this string as JSON if needed
        "\"" + node.asText().replace("\\", "\\\\").replace("\"", "\\\"") + "\""
      case JsonNodeType.NUMBER => node.asText()
      case nt@_ => logger.warn("Node type: $nt"); s"<UNKNOWN NODE TYPE: $nt>"
    }
  }

  private[this] def testnameToJsonPath(dataset:String, testName: String): Path = {
    Paths.get(Resources.getResource(s"queryresults/${dataset}/${testName}.json").toURI)
  }

  private[this] def loadTestResult(dataset:String, testName: String): JsonNode = {
    val resultPath = testnameToJsonPath(dataset, testName)
    logger.info("Comparing with expected result in: " + resultPath)
    mapper.readTree(resultPath.toFile)
  }

  private[this] def getTempFile(filename: String): Path = {
    Paths.get(System.getProperty("java.io.tmpdir"), filename)
  }

  def assertJsonEqual(dataset:String, testName: String, queryResult: Any) = {
    val expected: JsonNode = loadTestResult(dataset, testName)
    val expectedOrdered: String = toStringOrdered(expected)
    val actual: JsonNode = convertToJsonNode(queryResult)
    val actualOrdered: String = toStringOrdered(actual)
    if (expectedOrdered != actualOrdered) {
      val actualPath = getTempFile(testName + "_actual.json")
      val expectedPath = getTempFile(testName + "_expected.json")
      val actualOrderedPretty = prettyPrintTree(actualOrdered)
      val expectedOrderedPretty = prettyPrintTree(expectedOrdered)
      Files.write(actualPath, actualOrderedPretty.getBytes(StandardCharsets.UTF_8))
      Files.write(expectedPath, expectedOrderedPretty.getBytes(StandardCharsets.UTF_8))
      logger.warn(s"Test fail. Expected in file: ${expectedPath}. Actual in: $actualPath")
      fail(s"Results differ. Expected:\n$expectedOrderedPretty\n\nActual:\n$actualOrderedPretty")
    }
  }
}
