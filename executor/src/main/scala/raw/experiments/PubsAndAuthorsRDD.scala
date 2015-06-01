package raw.experiments

import java.io.PrintStream
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import raw.repl.RawSparkContext

import scala.collection.Map


object PubsAndAuthorsRDD extends StrictLogging {

  def printRDD(rdd: DataFrame): Unit = {
    println("Res: " + rdd.count())
    rdd.show(100)
  }

  val outFile = {
    val f = Files.createTempFile("publications-rdd-results", ".txt")
    logger.info("Logging results to {}", f)
    new PrintStream(Files.newOutputStream(f))
  }

  //  val sparkConf = new SparkConf()
  //    .setAppName("RAW Unit Tests")
  ////    .setMaster("local[2]")
  ////    .setMaster("spark://192.168.1.32:7077")
  //    // Disable compression to avoid polluting the tmp directory with dll files.
  //    // By default, Spark compresses the broadcast variables using the JavaSnappy. This library uses a native DLL which
  //    // gets copied as a new file to the TMP directory every time an instance of Spark is run.
  //    // http://spark.apache.org/docs/1.3.1/configuration.html#compression-and-serialization
  //    .set("spark.broadcast.compress", "false")
  //    .set("spark.shuffle.compress", "false")
  //    .set("spark.shuffle.spill.compress", "false")
  //  //      .set("spark.io.compression.codec", "lzf") //lz4, lzf, snappy
  //  val sparkContext: SparkContext = new SparkContext(sparkConf)
  val sparkContext: SparkContext = new RawSparkContext().sc

  //  lazy val sc = {
  //    logger.info("Starting local Spark context")
  //    new SparkContext(conf)
  //  }}

  val pubs: RDD[Publication] = Common.newRDDFromJSON[Publication]("data/pubs-authors/publications.json", sparkContext)
  val authors: RDD[Author] = Common.newRDDFromJSON[Author]("data/pubs-authors/authors.json", sparkContext)

  def outAndFile(str: String): Unit = {
    println(str)
    outFile.println(str)
    outFile.flush()
  }

  val repetitions = 5

  def doTest[T](rdd: RDD[T]): Unit = {
    outAndFile("*" * 80)
    //      outAndFile("Query: " + hql)

    // Do the test
    val stats = new DescriptiveStatistics()
    for (a <- 1 to repetitions) {
      logger.info("Test " + a)
      val c = Stopwatch.createStarted()
      rdd.count()
      stats.addValue(c.elapsed(TimeUnit.MILLISECONDS) / 1000.0)
    }

    outAndFile("Result sample for a total of " + rdd.count() + " rows")
    outAndFile(rdd.take(100).map(_.toString.take(80)).mkString("\n"))
    outAndFile(f"ExecutionTime=${stats.getMean}%5.2f, stddev=${stats.getStandardDeviation}%5.2f, repeats=$repetitions%d")
    outAndFile(s"Times: ${stats.getValues.mkString(", ")}")
  }

  // Articles which have more than one PhD student as an author.
  // Assumes that the set of phds is small enough to be broadcasted to all nodes.
  // Otherwise, must do a join
  //    logger.info("Local phds: " + localPhds.mkString("\n"))
  def queryArticlesWithMoreThanOnePhDStudent() = {
    val phds: RDD[String] = authors.filter(a => a.title == "PhD").map(a => a.name)
    val localPhds: Set[String] = phds.toLocalIterator.toSet
    val phdsBcasted: Broadcast[Set[String]] = sparkContext.broadcast(localPhds)
    val results: RDD[String] = pubs
      .filter(p => p.authors.count(p => phdsBcasted.value.contains(p)) > 1)
      .map(p => p.title)
    results
  }

  def articlesAllAuthorsSameAge(): RDD[Publication] = {
    val authorsYear = authors.map(a => (a.name, a.year)).collectAsMap()
    val bcastVar = sparkContext.broadcast(authorsYear)
    pubs.filter(p => {
      val authorAges = p.authors.map(a => bcastVar.value.get(a))
      authorAges.toSet.size == 1
    })
  }

  // Articles where one is a prof, one is a PhD student, but the prof is younger than the PhD student.
  def oneProfOneStudentProfYoungerThanStudent(): RDD[Publication] = {
    val authorsMap: Map[String, Author] = authors.map(a => a.name -> a).collectAsMap()
    val bcasted = sparkContext.broadcast(authorsMap)
    val v: RDD[Publication] = pubs.filter(p => {
      val groups = p.authors.groupBy(name => bcasted.value.get(name).get.title)
      println("Groups: " + groups)
//      val namesOfPhDsAuthors = p.authors.filter(name => bcasted.value.get(name).get.title == "PhD")
//      val namesOfProfAuthors = p.authors.filter(name => bcasted.value.get(name).get.title == "professor")
      val count = for {
        phd <- groups.get("PhD").get.map(name => bcasted.value.get(name).get)
        prof <- groups.get("professor").get.map(name => bcasted.value.get(name).get)
        if (prof.year > phd.year)
      } yield (phd, prof)
      if (!count.isEmpty) {
        println("Matches for article " + p.title)
        count.foreach(pair => println("\t" + pair))
      }
      !count.isEmpty
    })
    v
  }

  def main(args: Array[String]) {
    // val res = queryArticlesWithMoreThanOnePhDStudent()
    // val res = articlesAllAuthorsSameAge()
    val res = oneProfOneStudentProfYoungerThanStudent()
    logger.info("Result size: " + res.count())
    logger.info("result: " + res.collect().mkString("\n"))
  }
}
