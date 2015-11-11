package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestScanners
import raw.executor.DropboxAuthUsers._
import raw.rest.DefaultJsonMapper
import raw.rest.RawService.QueryBlockResponse

import scala.collection.mutable.ListBuffer

/*
Tests:
- Client does not iterate to end of results: server must clean resources after timeout.
 */
class RestServerPaginationTest extends FunSuite with RawRestServerContext with StrictLogging with BeforeAndAfterAll {

  override def beforeAll() = {
    super.beforeAll()
    clientProxy.registerLocalFile(TestUserJoe, TestScanners.authorsPath, "authors")
  }

  def printBlock(block: QueryBlockResponse) = {
    logger.info(s"QueryBlockResponse(${block.start}+${block.size}, hasMore: ${block.hasMore}).")
  }

  def testPagination(query: String, resultsPerPage: Int, expectedTotalResults: Int) = {
    var block: QueryBlockResponse = clientProxy.queryStart(query, resultsPerPage, DropboxAuthUsers.TestUserJoe)
    val results = new ListBuffer[String]()
    var stop = false
    while (!stop) {
      logger.info(s"This block: ${DefaultJsonMapper.mapper.writeValueAsString(block.data)}")
      printBlock(block)
      val data = block.data.asInstanceOf[Seq[String]]
      val nextExpectedPageSize = Math.min(resultsPerPage, expectedTotalResults - results.length)
      assert(block.size === nextExpectedPageSize)
      results ++= data
      if (block.hasMore) {
        block = clientProxy.queryNext(block.token, resultsPerPage, DropboxAuthUsers.TestUserJoe)
      } else {
        stop = true
      }
    }
    logger.info(s"All results: $results")
    assert(results.size === expectedTotalResults)
  }

  test("pagination10") {
    testPagination("select a.name from authors a", 10, 50)
  }

  test("pagination5") {
    testPagination("select a.name from authors a", 5, 50)
  }

  test("pagination13") {
    testPagination("select a.name from authors a", 13, 50)
  }

  test("no results") {
    var block: QueryBlockResponse = clientProxy.queryStart( """select * from authors a where a.name = "Joe Doe"  """, 10, DropboxAuthUsers.TestUserJoe)
    assert(block.data == null, s"block")
    assert(block.start == 0, s"$block")
    assert(block.size == 0, s"$block")
    assert(!block.hasMore, s"block")
    assert(block.token == null, s"$block")
  }

  test("one page of results") {
    var block: QueryBlockResponse = clientProxy.queryStart( """select a.name from authors a where a.title = "engineer"  """, 10, DropboxAuthUsers.TestUserJoe)
    val actual = block.data.asInstanceOf[List[String]].toSet
    val expected = Set("Lee, A.", "Ishibashi, K.", "Matsumoto, Y.", "Gallion, P.", "Xu, Rongrong")
    assert(actual === expected)
    assert(block.start == 0, s"$block")
    assert(block.size == 5, s"$block")
    assert(!block.hasMore, s"block")
    assert(block.token == null, s"$block")
  }


  test("invalid resultsPerPage") {
    try {
      var block: QueryBlockResponse = clientProxy.queryStart( """select a.name from authors a""", -10, DropboxAuthUsers.TestUserJoe)
      fail("Should have failed with exception. Instead call succeeded. Response: " + block)
    } catch {
      case ex: ClientErrorWrapperException => {
        assert(ex.exceptionResponse.exceptionType === "raw.rest.ClientErrorException")
      }
      case ex: Throwable => fail("Unexpected exception: " + ex)
    }
  }


  test("iterate past the end") {
    // This query returns 16 results, so it should be in two pages.
    var block: QueryBlockResponse = clientProxy.queryStart( """select a.name from authors a where a.title = "PhD"  """, 10, DropboxAuthUsers.TestUserJoe)
    val token = block.token
    assert(block.hasMore, s"block")
    // Second page, 6 results
    block = clientProxy.queryNext(token, 10, DropboxAuthUsers.TestUserJoe)
    try {
      // This call will fail
      block = clientProxy.queryNext(token, 10, DropboxAuthUsers.TestUserJoe)
      fail("Should have failed with exception. Instead call succeeded. Response: " + block)
    } catch {
      case ex: ClientErrorWrapperException => {
        assert(ex.exceptionResponse.exceptionType === "raw.rest.ClientErrorException")
      }
      case ex: Throwable => fail("Unexpected exception: " + ex)
    }
  }

  test("single primitive result") {
    var block: QueryBlockResponse = clientProxy.queryStart(
      """select a.year from authors a where a.name = "Stricker, D.A."  """,
      10,
      DropboxAuthUsers.TestUserJoe)
    assert(block.data === List(1972))
    assert(block.size === 1)
    assert(block.hasMore === false)
    assert(block.start === 0)
    assert(block.token === null)
  }
}