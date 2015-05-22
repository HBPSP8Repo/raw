package raw.repl

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}

class RawSparkContext extends StrictLogging with AutoCloseable {
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
//      .set("spark.io.compression.codec", "lzf") //lz4, lzf, snappy

  lazy val sc = {
    logger.info("Starting local Spark context")
    new SparkContext(conf)
  }

  override def close() {
    logger.info("Stopping Spark context")
    sc.stop()
  }
}
