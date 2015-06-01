package raw.experiments

import java.io.PrintStream
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import raw.repl.RawSparkContext

import scala.collection.mutable

object PublicationsRDD extends StrictLogging {

  def printRDD(rdd: DataFrame): Unit = {
    println("Res: " + rdd.count())
    rdd.show(100)
  }

  val outFile = {
    val f = Files.createTempFile("publications-rdd-results", ".txt")
    logger.info("Logging results to {}", f)
    new PrintStream(Files.newOutputStream(f))
  }

  val sparkContext: SparkContext = new RawSparkContext().sc

  val pubs = Common.newRDDFromJSON[Publication]("data/publicationsConverted.json", sparkContext)

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

  def queryXXX(): RDD[String] = {
    //    runQuery( """SELECT DISTINCT explode(authors) FROM pubs
    //	    WHERE array_contains(controlledterms, "particle detectors")""")
    pubs.filter(p => p.controlledterms.contains("particle detectors"))
      .flatMap(p => p.authors)
      .distinct()
  }

  //  Articles grouped by number of authors
  //  SELECT authCount, collect_list(title) docs
  //    FROM (SELECT title, size(authors) AS authCount FROM pubs) t2
  //    GROUP BY authCount
  def query1(): RDD[(Int, Iterable[String])] = {
    pubs
      .map(p => (p.authors.size, p.title))
      .groupByKey()
      .sortByKey()
  }

  //  SELECT authCount, count(title) articles
  //    FROM (SELECT title, size(authors) as authCount from pubs) t2
  //    GROUP BY authCount
  def query2(): RDD[(Int, Int)] = {
    pubs
      .map(p => (p.authors.size, 1))
      .reduceByKey((a, b) => a + b)
      .sortByKey()
  }

  //  SELECT author, collect_list(title)
  //    FROM pubs LATERAL VIEW explode(authors) t2 AS author
  //    GROUP BY author
  //    ORDER BY author
  def query3(): RDD[(String, Iterable[String])] = {
    pubs
      .flatMap(p => p.authors.map(a => (a, p.title)))
      .groupByKey()
      .sortByKey()
  }

  //  SELECT author, count(title) AS titles
  //  FROM pubs LATERAL VIEW explode(authors) t2 AS author
  //    GROUP BY author
  //    ORDER BY author
  def query4(): RDD[(String, Int)] = {
    pubs
      .flatMap(p => p.authors.map(a => (a, 1)))
      .reduceByKey((a, b) => a + b)
      .sortByKey()
  }

  //    runQuery( """SELECT t2.term, collect_list(title) AS titles
  //      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
  //      GROUP BY t2.term""")
  def query5(): RDD[(String, Iterable[String])] = {
    pubs
      .flatMap(p => p.controlledterms.map(term => (term, p.title)))
      .groupByKey()
  }

  //    runQuery( """SELECT t2.term, count(title) AS titles
  //      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
  //      GROUP BY t2.term """)
  def query6() = {
    pubs
      .flatMap(p => p.controlledterms.map(term => (term, 1)))
      .reduceByKey((a, b) => a + b)
  }

  //    runQuery( """SELECT DISTINCT explode(authors) FROM pubs
  //	    WHERE array_contains(controlledterms, "particle detectors")""")
  def query7a(): RDD[String] = {
    pubs.filter(p => p.controlledterms.contains("particle detectors"))
      .flatMap(p => p.authors)
      .distinct()
  }

  def query7b(): RDD[String] = {
    pubs.filter(p => p.controlledterms.contains("particle detectors"))
      .mapPartitions(iter => {
      val set = mutable.HashSet[String]()
      iter.foreach(p => set ++= p.authors)
      set.toIterator
    })
      .distinct()
  }

  // Faster, extracts and collects the results in a single stage.
  def query7c(): Set[String] = {
    val clock = Stopwatch.createStarted()
    val res = pubs.filter(p => p.controlledterms.contains("particle detectors"))
      .aggregate(mutable.HashSet[String]())(
        (set, pub) => set ++= pub.authors,
        (s1, s2) => s1 ++= s2)
    val time = clock.elapsed(TimeUnit.MILLISECONDS)
    println("Result: " + res.size + ": " + res.mkString("\n") + "\nTime: " + time)
    res.toSet
  }

  //        runQuery( """SELECT author, count(title) as publications
  //    	    FROM pubs LATERAL VIEW explode(authors) t2 AS author
  //    	    WHERE array_contains(controlledterms, "particle detectors")
  //    	    GROUP BY author""")
  def query8a() = {
    pubs
      .filter(p => p.controlledterms.contains("particle detectors"))
      .flatMap(p => p.authors.map(author => (author, 1)))
      .reduceByKey((a, b) => a + b)
  }

  // Does not seem to improve performance compared to simple version above.
  def query8b() = {
    pubs
      .filter(p => p.controlledterms.contains("particle detectors"))
      .mapPartitions(iter => {
      val counts = mutable.HashMap[String, Int]()
      iter.foreach(p => p.authors.foreach(a => {
        val oldVal = counts.getOrElse(a, 0)
        counts.update(a, oldVal + 1)
      }))
      counts.toIterator
    })
      .reduceByKey((a, b) => a + b)
  }

  def main(args: Array[String]) {
    logger.info("Create rdd: " + pubs)
    doTest(query8b())
    //    Thread.sleep(60000)
    //    scala.io.StdIn.readLine()
  }
}
