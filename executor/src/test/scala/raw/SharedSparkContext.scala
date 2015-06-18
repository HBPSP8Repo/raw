package raw

import java.nio.file.Paths

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

object SharedSparkContext {
  private[this] val metricsConf = Paths.get(Resources.getResource( """metrics.properties""").toURI)
  val conf = new SparkConf()
    .setAppName("RAW Unit Tests")
    .setMaster("local[4]")
    // Disable compression to avoid polluting the tmp directory with dll files.
    // By default, Spark compresses the broadcast variables using the JavaSnappy. This library uses a native DLL which
    // gets copied as a new file to the TMP directory every time an instance of Spark is run.
    // http://spark.apache.org/docs/1.3.1/configuration.html#compression-and-serialization
    .set("spark.broadcast.compress", "false")
    .set("spark.shuffle.compress", "false")
    .set("spark.shuffle.spill.compress", "false")

    //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    .registerKryoClasses(Array(classOf[Publication], classOf[Author], classOf[QueryResult]))

    // https://spark.apache.org/docs/1.3.1/monitoring.html
    .set("spark.eventLog.enabled", "true")
    //    .set("spark.eventLog.dir", "file:///")

    .set("spark.metrics.conf", metricsConf.toString)

    // Spark SQL configuration
    //  https://spark.apache.org/docs/latest/sql-programming-guide.html
    //  spark.sql.codegen
    //  spark.sql.autoBroadcastJoinThreshold
    .set("spark.sql.shuffle.partitions", "10") // By default it's 200, which is large for small datasets
  //      .set("spark.io.compression.codec", "lzf") //lz4, lzf, snappy

}

/* Create a new Spark context for every test suite.
This allows us to use the afterAll() callback to close Spark at the end of the test suit.
An alternative design would be to use a single Spark context for all the test run, by creating it inside an object.
The problem with this design is how to ensure that the Spark context is always closed at the end of all the tests,
AFAIK, there is no callback in the ScalaTests to execute code when the tests end. Another problem is the loss of
independence between test suites.
The first problem can be mitigated by ensuring that the JVM is always closed at the end of the tests (fork := true)
in SBT.
*/
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  // Override to modify configuration
  var conf = SharedSparkContext.conf

  override def beforeAll() {
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
