package raw.experiments

import java.io.PrintStream
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Stopwatch
import com.google.common.io.{LineProcessor, Resources}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import raw.experiments.PubsAndAuthorsSQL._

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Author(name: String, title: String, year: Int)

object Common extends StrictLogging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def newRDDFromJSON[T](resource: String, sparkContext: SparkContext)(implicit ct: ClassTag[T]) = {
    logger.info(s"Loading resource: $resource")
    val url: URL = Resources.getResource(resource)
    logger.info(s"URL: $url")

    val lineProcessor = new LineProcessor[Seq[T]] {
      val tref = new TypeReference[T] {
        override def getType = ct.runtimeClass
      }
      val lines = new ArrayBuffer[T]()

      override def processLine(s: String): Boolean = {
        lines += mapper.readValue(s, tref)
        true
      }

      override def getResult = lines
    }
    val lines: Seq[T] = Resources.readLines(url, StandardCharsets.UTF_8, lineProcessor)

    val start = Stopwatch.createStarted()
    val rdd: RDD[T] = sparkContext.parallelize(lines)
    rdd.persist()
    val inputSize = rdd.count() // Force the RDD to be cached into memory, as persist is lazy
    logger.info(s"Loaded $inputSize elements from $resource in ${start.elapsed(TimeUnit.MILLISECONDS)}")
    rdd


    //    logger.info(s"URI: $uri")
    //    val start = Stopwatch.createStarted()
    //    val path: Path = Paths.get(uri)
    //    val rdd: RDD[T] = sparkContext
    //      .textFile(path.toString)
    //      .mapPartitions(iter => {
    //      val mapper = new ObjectMapper()
    //      mapper.registerModule(DefaultScalaModule)
    //      val tref = new TypeReference[T] {
    //        override def getType = ct.runtimeClass
    //      }
    //      iter.map(line => mapper.readValue[T](line, tref))
    //    }
    //      )
    //    rdd.persist()
    //    val inputSize = rdd.count() // Force the RDD to be cached into memory, as persist is lazy
    //    logger.info(s"Loaded $inputSize elements from $resource in ${start.elapsed(TimeUnit.MILLISECONDS)}")
    //    rdd
  }


  def newDataframeFromJSON(resource: String, hiveContext: HiveContext): DataFrame = {
    logger.info(s"Loading resource: $resource")
    val path: Path = Paths.get(Resources.getResource(resource).toURI)
    val filename = path.getFileName.toString.takeWhile(c => c != '.')
    val pubs: DataFrame = hiveContext.jsonFile(path.toString)
    pubs.registerTempTable(filename)
    pubs.persist()
    val count = pubs.count() // Force the RDD to be cached into memory, as persist is lazy
    logger.info(s"Registered table $filename with $count rows")
    pubs
  }

  def time[A](a: => A) = {
    val now = System.nanoTime
    val result = a
    val micros = (System.nanoTime - now) / 1000000
    println("%d ms".format(micros))
    result
  }

  // TODO: Save the file as a separate JSON per line, so Spark can read it directly.
  def convertToOneLinePerJSON() = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val path: Path = Paths.get(Resources.getResource("data/publications.json").toURI)
    logger.info("Loading file: {}", path)
    // https://www.google.ch/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0CCUQFjAB&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F187&ei=k9xZVa-GEsX0ULbVgbgK&usg=AFQjCNEU-uoMGTlGyhJtbnMNNqGNAjMa2Q&sig2=xGEikKmA0eElXE6quoVdrQ&bvm=bv.93564037,d.d24
    //    val values: List[Map[String, Any]] = mapper.readValue(Files.newInputStream(path), classOf[List[Map[String, Any]]])
    //    val mapper = new ObjectMapper() with ScalaObjectMapper
    //    val values = mapper.readValue[Seq[Publication]](Files.newBufferedReader(path))
    //    val values: Seq[Publication] = mapper.readValue(Files.newBufferedReader(path), new TypeReference[Seq[Publication]]() {})
    val jsonList: Seq[Publication] = mapper.readValue(Files.newBufferedReader(path), new TypeReference[Seq[Publication]]() {})
    val strList = jsonList.map(mapper.writeValueAsString(_))
    Files.write(Paths.get("publicationsConverted.json"), JavaConversions.asJavaIterable(strList), StandardCharsets.UTF_8)
  }

  def printRDD(rdd: DataFrame): Unit = {
    println("Res: " + rdd.count())
    rdd.show(100)
  }

  val outFile = {
    val f = Files.createTempFile("publications-sql-results", ".txt")
    logger.info("Logging results to {}", f)
    new PrintStream(Files.newOutputStream(f))
  }

  def outAndFile(str: String): Unit = {
    println(str)
    outFile.println(str)
    outFile.flush()
  }

  def runQuery(hql: String, repetitions: Int = 5): Unit = {
    outAndFile("*" * 80)
    outAndFile("Query: " + hql)
    val clock = Stopwatch.createStarted()
    val df = hiveContext.sql(hql)
    val prepareTime = clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0

    // Do the test
    val stats = new DescriptiveStatistics()
    for (a <- 1 to repetitions) {
      val c = Stopwatch.createStarted()
      df.count()
      stats.addValue(c.elapsed(TimeUnit.MILLISECONDS) / 1000.0)
    }

    // Warm up by printing the query plan and a sample of 20 results
    Console.withOut(outFile) {
      df.explain(true)
      val rowsToShow = 100
      println(s"Showing first $rowsToShow rows out of a total of ${df.count()} rows")
      df.show(100)
    }

    outAndFile(f"prepareTime=${prepareTime}%5.2f, ExecutionTime=${stats.getMean}%5.2f, " +
      f"stddev=${stats.getStandardDeviation}%5.2f, repeats=$repetitions%d")
    outAndFile(s"Times: ${stats.getValues.mkString(", ")}")
  }

}
