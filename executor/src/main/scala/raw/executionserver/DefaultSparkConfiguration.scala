package raw.executionserver

import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import com.google.common.base.Stopwatch
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import raw.datasets.patients.{Diagnostic, Patient}
import raw.datasets.publications.{Author, Publication}

import scala.reflect.ClassTag

object DefaultSparkConfiguration extends StrictLogging {
  private[this] val metricsConf = Paths.get(Resources.getResource("""metrics.properties""").toURI)
  private[this] val eventDirectory = {
    val dir = Paths.get(System.getProperty("java.io.tmpdir"), "spark-events")

    try {
      Files.createDirectory(dir)
    } catch {
      //Ignore, already exists
      case ex: FileAlreadyExistsException => //ignore, normal
    }
    dir
  }

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

    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[Publication], classOf[Author], classOf[Patient], classOf[Diagnostic]))
    //    .set("spark.kryo.referenceTracking", "false")

    // https://spark.apache.org/docs/1.3.1/monitoring.html
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", eventDirectory.toString)
    .set("spark.metrics.conf", metricsConf.toString)

    /* codegen is still buggy:
    the query below produces the wrong results, where the results do not contain all the keywords.
    Additionally, execution is not consistent, it produces different number of results in each run.
      select * from publications
      where array_contains(controlledterms, "particle detectors")
      and array_contains(controlledterms, "elemental semiconductors")
      and array_contains(controlledterms, "magnetic levitation")
      and array_contains(controlledterms, "titanium")
      and array_contains(controlledterms, "torque")
      */
    //    .set("spark.sql.codegen", "true")

    // Spark SQL configuration
    //  https://spark.apache.org/docs/latest/sql-programming-guide.html
    //  spark.sql.codegen
    //  spark.sql.autoBroadcastJoinThreshold
    .set("spark.sql.shuffle.partitions", "10") // By default it's 200, which is large for small datasets
  //      .set("spark.io.compression.codec", "lzf") //lz4, lzf, snappy

  def newRDDFromJSON[T <: Product : ClassTag](lines: List[T], sparkContext: SparkContext): RDD[T] = {
    val start = Stopwatch.createStarted()
    val rdd: RDD[T] = sparkContext.parallelize(lines)
    logger.info("Created RDD. Partitions: " + rdd.partitions.map(p => p.index).mkString(", ") + ", partitioner: " + rdd.partitioner)
    rdd
  }
}