package raw

import java.net.URL

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, Suite}
import raw.executionserver.{RawClassLoader, DefaultSparkConfiguration, RawMutableURLClassLoader}


/* Create a new Spark context for every test suite.
This allows us to use the afterAll() callback to close Spark at the end of the test suit.
An alternative design would be to use a single Spark context for all the test run, by creating it inside an object.
The problem with this design is how to ensure that the Spark context is always closed at the end of all the tests,
AFAIK, there is no callback in the ScalaTests to execute code when the tests end. Another problem is the loss of
independence between test suites.
The first problem can be mitigated by ensuring that the JVM is always closed at the end of the tests (fork := true)
in SBT.
*/
trait SharedSparkContext extends BeforeAndAfterAll with StrictLogging {
  self: Suite with RawClassLoader =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  // Override to modify configuration
  var conf = DefaultSparkConfiguration.conf

  override def beforeAll() {
    logger.info("Creating raw class loader: " + rawClassLoader)
    Thread.currentThread().setContextClassLoader(rawClassLoader)
    logger.info("Starting SparkContext with configuration:\n{}", conf.toDebugString)
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    _sc = null
    super.afterAll()
  }
}


trait SharedSparkSQLContext extends SharedSparkContext {
  self: Suite with RawClassLoader =>

  var sqlContext: SQLContext = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }
}