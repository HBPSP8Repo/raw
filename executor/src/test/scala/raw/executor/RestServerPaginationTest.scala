package raw.executor

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import raw.TestScanners
import raw.executor.DropboxAuthUsers._
import raw.rest.DefaultJsonMapper
import raw.rest.RawService.QueryBlockResponse

/*
Tests:
- Empty results
- Primitive results
- Results do not fill first block
- Iterate past the end: throw exception
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

  test("pagination") {
    // TODO: Check results.
    var block: QueryBlockResponse = clientProxy.queryStart("select a.name from authors a", DropboxAuthUsers.TestUserJoe)
    printBlock(block)
    while (block.hasMore) {
      block = clientProxy.queryNext(block.token, DropboxAuthUsers.TestUserJoe)
      logger.info(s"This block: ${DefaultJsonMapper.mapper.writeValueAsString(block.data)}")
      printBlock(block)
    }
    // Check we get 50 results
  }

  test("no results") {
    var block: QueryBlockResponse = clientProxy.queryStart("""select * from authors a where a.name = "Joe Doe"  """, DropboxAuthUsers.TestUserJoe)
    assert(block.data == null, s"block")
    assert(block.start == 0, s"$block")
    assert(block.size == 0, s"$block")
    assert(!block.hasMore, s"block")
    assert(block.token == null, s"$block")
  }

  test("one page of results") {
    var block: QueryBlockResponse = clientProxy.queryStart("""select a.name from authors a where a.title = "engineer"  """, DropboxAuthUsers.TestUserJoe)
    val actual = block.data.asInstanceOf[List[String]].toSet
    val expected = Set("Lee, A.", "Ishibashi, K.", "Matsumoto, Y.", "Gallion, P.", "Xu, Rongrong")
    assert(actual === expected)
    assert(block.start == 0, s"$block")
    assert(block.size == 5, s"$block")
    assert(!block.hasMore, s"block")
    assert(block.token == null, s"$block")
  }
}