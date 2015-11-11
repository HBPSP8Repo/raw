package raw.rest

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.cache._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import raw.{RawQuery, RawQueryIterator}


object QueryCache extends StrictLogging {
  final val DEFAULT_RESULTS_PER_PAGE = 10
  final val timeout: Long = ConfigFactory.load().getDuration("raw.querycache.timeout", TimeUnit.SECONDS)
  final val maximumSize: Int = ConfigFactory.load().getInt("raw.querycache.maximumSize")
  logger.info(s" Timeout: $timeout; maximumSize: $maximumSize")
}

/*
 * Stores open query iterators used by paginated queries. The iterators are kept until one of: the client finishes
 * iterating through the results, the iterator is not accessed for more than a certain timeout, the number of iterators
 * cached exceeds the maximum limit (evicts the least recently accessed iterator)
 */
class QueryCache extends StrictLogging {

  import QueryCache._

  // When an entry in the cache is removed or invalidated, it will be reclaimed as part of the periodic
  // maintenance activities of the cache. This can take a long time. Here we force periodic reclamation
  // by calling cleanUp() periodically.
  Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        queryCache.cleanUp()
      }
    }, 5, 5, TimeUnit.SECONDS)

  class OpenConnectionRemovalListener extends RemovalListener[String, OpenQuery] {
    override def onRemoval(removalNotification: RemovalNotification[String, OpenQuery]): Unit = {
      logger.info(s"Removed query iterator: $removalNotification, Cause: ${removalNotification.getCause}.")
      removalNotification.getValue.close()
    }
  }

  // Uses Guava's Cache implementation for its support expiration and maximum size.
  private[this] val queryCache: Cache[String, OpenQuery] = CacheBuilder.newBuilder()
    .maximumSize(maximumSize)
    .expireAfterAccess(timeout, TimeUnit.SECONDS)
    .removalListener(new OpenConnectionRemovalListener())
    .build()

  def newOpenQuery(query: String, rawQuery: RawQuery): OpenQuery = {
    val iterator: RawQueryIterator = rawQuery.iterator
    val token = UUID.randomUUID().toString
    new OpenQuery(token, query, rawQuery, iterator)
  }

  def put(openQuery: OpenQuery): Unit = {
    queryCache.put(openQuery.token, openQuery)
  }

  def get(token: String): Option[OpenQuery] = {
    Option(queryCache.getIfPresent(token))
  }

  class OpenQuery(val token: String, val query: String, val rawQuery: RawQuery,
                  val iterator: RawQueryIterator) {
    // -1 indicates query is closed.
    private[this] var _position: Int = 0

    // Give read-only access
    def position = _position

    def hasNext: Boolean = {
      val b = iterator.hasNext
      if (!b) {
        close()
      }
      b
    }

    def next(resultsPerPage: Int = DEFAULT_RESULTS_PER_PAGE): List[Any] = {
      if (resultsPerPage <= 0) {
        throw new ClientErrorException(s"""Invalid argument, "resultsPerPage" must be positive. Was: $resultsPerPage""")
      }
      if (_position == -1) {
        throw new ClientErrorException("Iterator closed")
      }
      if (!hasNext) {
        throw new ClientErrorException("No more data available")
      }
      val nextBlock: List[Any] = iterator.take(resultsPerPage).toList
      _position += nextBlock.length
      nextBlock
    }

    def close() = {
      if (_position != -1) {
        queryCache.invalidate(token)
        iterator.close()
        _position = -1
      }
    }

    override def toString(): String = {
      s"token: $token, position: ${_position}, query: $query"
    }
  }
}

