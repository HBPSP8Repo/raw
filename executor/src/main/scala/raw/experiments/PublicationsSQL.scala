package raw.experiments

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Stopwatch
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import raw.repl.RawSparkContext

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

object PublicationsDataframe extends StrictLogging {

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

  def authors(rdd: DataFrame) = {
    val v: DataFrame = rdd.select("authors").limit(10)
    val rows: Array[Row] = v.collect()
    val s: Array[ArrayBuffer[String]] = rows.map(row => row.getAs[ArrayBuffer[String]](0))
    s.foreach(ab => println("authors: " + ab.mkString("; ")))
  }

  def printRDD(rdd: DataFrame): Unit = {
    println("Res: " + rdd.count())
    rdd.show(100)
  }

  // select doc from doc in Documents where "particle detectors" in controlledterms
  def query1(hiveContext: HiveContext, pubs: DataFrame): Unit = {
    val res = hiveContext.sql( """ SELECT title FROM pubs WHERE array_contains(controlledterms, "particle detectors") """)
    printRDD(res)
  }

  val outFile = {
    val f = Files.createTempFile("publications-sql-results", ".txt")
    logger.info("Logging results to {}", f)
    new PrintStream(Files.newOutputStream(f))
  }

  val hiveContext: HiveContext = {
    val rawSparkContext = new RawSparkContext
    new HiveContext(rawSparkContext.sc)
  }

  val pubs: DataFrame = {
    val path: Path = Paths.get(Resources.getResource("data/publicationsConverted.json").toURI)
    val pubs: DataFrame = hiveContext.jsonFile(path.toString)
    pubs.registerTempTable("pubs")
    pubs.persist()
    pubs.count() // Force the RDD to be cached into memory, as persist is lazy
    pubs
  }

  def outAndFile(str: String): Unit = {
    println(str)
    outFile.println(str)
    outFile.flush()
  }

  val repetitions = 5
  def runQuery(hql: String): Unit = {
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

  def main(args: Array[String]) {
    //    createDataFrame[Publication](data).persist()
    //    val rdd: RDD[String] = rawSparkContext.sc.parallelize(data, 4)
    //    query1(hiveContext, pubs)

    logger.info("Create rdd: " + pubs)
//    runQuery( """ SELECT title, author FROM pubs LATERAL VIEW explode(authors) authorsTable AS author""")
//
//    runQuery( """SELECT authCount, collect_list(title) docs
//	    FROM (SELECT title, size(authors) AS authCount FROM pubs) t2
//	    GROUP BY authCount
//	    ORDER BY authCount""")
//
//    runQuery( """SELECT authCount, count(title) articles
//      FROM (SELECT title, size(authors) as authCount from pubs) t2
//      GROUP BY authCount
//      ORDER BY authCount""")
//
//    runQuery( """SELECT author, collect_list(title)
//      FROM pubs LATERAL VIEW explode(authors) t2 AS author
//      GROUP BY author
//      ORDER BY author""")
//
//    runQuery( """SELECT author, count(title) AS titles
//      FROM pubs LATERAL VIEW explode(authors) t2 AS author
//      GROUP BY author
//      ORDER BY author""")
//
//    runQuery( """SELECT t2.term, collect_list(title) AS titles
//      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
//      GROUP BY t2.term""")
//
//    runQuery( """SELECT t2.term, count(title) AS titles
//      FROM pubs LATERAL VIEW explode(controlledterms) t2 AS term
//      GROUP BY t2.term """)
//
//    runQuery( """SELECT DISTINCT explode(authors) FROM pubs
//	    WHERE array_contains(controlledterms, "particle detectors")""")
//    runSparkJob()

//    runQuery( """SELECT author, count(title) as publications
//	    FROM pubs LATERAL VIEW explode(authors) t2 AS author
//	    WHERE array_contains(controlledterms, "particle detectors")
//	    GROUP BY author ORDER BY author""")
    outFile.close()
  }
}
