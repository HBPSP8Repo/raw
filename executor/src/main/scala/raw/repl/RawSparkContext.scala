package raw.repl

import java.nio.file.Paths

import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}

class RawSparkContext extends StrictLogging with AutoCloseable {

  private[this] val metricsConf = Paths.get(Resources.getResource( """metrics.properties""").toURI)
  lazy val conf = new SparkConf()
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


  lazy val sc = {
    logger.info("Starting local Spark context with configuration:\n" + conf.toDebugString)
    new SparkContext(conf)
  }

  override def close() {
    logger.info("Stopping Spark context")
    sc.stop()
  }
}
