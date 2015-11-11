package raw.rest

import java.util.UUID

import raw.{RawQuery, RawQueryIterator}


object QueryCache {
  final val DEFAULT_RESULTS_PER_PAGE = 10
}

class QueryCache {

  import QueryCache._

  private[this] val queryCache = new scala.collection.mutable.HashMap[String, OpenQuery]()

  def newOpenQuery(query: String, rawQuery: RawQuery): OpenQuery = {
    val iterator: RawQueryIterator = rawQuery.iterator
    val token = UUID.randomUUID().toString
    new OpenQuery(token, query, rawQuery, iterator)
  }

  def put(openQuery: OpenQuery): Unit = {
    queryCache.put(openQuery.token, openQuery)
  }

  def get(token: String): Option[OpenQuery] = {
    queryCache.get(token)
  }

  class OpenQuery(val token: String, val query: String, val rawQuery: RawQuery,
                  val iterator: RawQueryIterator) {
    var position: Int = 0

    def hasNext: Boolean = {
      val b = iterator.hasNext
      if (!b) {
        iterator.close()
      }
      b
    }

    def next(resultsPerPage: Int = DEFAULT_RESULTS_PER_PAGE): List[Any] = {
      if (resultsPerPage <= 0) {
        throw new ClientErrorException(s"resultsPerPage must be positive. Was: $resultsPerPage")
      }
      if (position == -1) {
        throw new ClientErrorException("Iterator already closed")
      }
      if (!hasNext) {
        throw new ClientErrorException("No more elements available")
      }
      val nextBlock: List[Any] = iterator.take(resultsPerPage).toList
      position += nextBlock.length
      nextBlock
    }

    def close() = {
      queryCache.remove(token)
      iterator.close()
      position = -1
    }
  }

}
