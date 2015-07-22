package raw.perf

import java.io.PrintStream
import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.google.common.io.Resources
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.rogach.scallop.ScallopConf
import raw.RawQuery
import raw.datasets.publications.Publications
import raw.datasets.{AccessPath, Dataset}
import raw.executionserver.{DefaultSparkConfiguration, RawMutableURLClassLoader, ResultConverter}

import scala.collection.mutable.ArrayBuffer

object Queries {

  case class Query(name: String, oql: String, hql: String)

  def formatQuery(query: String): String = {
    // Remove the whitespace at the start of each line
    query.trim.replaceAll("\\n\\s+", "\n")
  }

  def readQueries(file: String): Seq[Query] = {
    val x = scala.xml.XML.loadFile(file)
    val queries = (x \ "query")
      .map(queryNode =>
      Query((queryNode \ "@name").text.trim, formatQuery((queryNode \ "oql").text), formatQuery((queryNode \ "hql").text)))
    queries.toSeq
  }

  case class QueryResult(avg: Double, min: Double, max: Double, stdDev: Double)

}

object PerfMain extends StrictLogging with ResultConverter {
  def setDockerHost(): Unit = {
    val dockerAddress = System.getenv().get("DOCKER_HOST")
    val ldbServerAddress = if (dockerAddress == null) {
      println("WARN: No DOCKER_HOST environment variable found. Using default of localhost for LDB compilation server")
      "http://localhost:5001/raw-plan"
    } else {
      println("Docker host: " + dockerAddress)
      val uri = new URI(dockerAddress)
      s"http://${uri.getHost}:5001/raw-plan"
    }
    println(s"RAW compilation server at $ldbServerAddress")
    System.setProperty("raw.compile.server.host", ldbServerAddress)
  }

  // This custom classloader must be created and set as the context classloader
  // for the main thread before the Spark context is created.
  val rawClassLoader = {
    val tmp = new RawMutableURLClassLoader(new Array[URL](0), PerfMain.getClass.getClassLoader)
    Thread.currentThread().setContextClassLoader(tmp)
    tmp
  }

  lazy val sc: SparkContext = {
    logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
    new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
  }

  /*
   * SparkSQL
   */
  lazy val sqlContext: HiveContext = new HiveContext(sc)

  def readJson(resource: String, tableName: String): DataFrame = {
    val url = Resources.getResource(resource)
    val fullpath = Paths.get(url.toURI).toString
    val df = sqlContext.read.json(fullpath)
    df.registerTempTable(tableName)
    df
  }

  def statsToString(stats: DescriptiveStatistics): String = {
    f"${stats.getMean}%6.3f, StdDev: ${stats.getStandardDeviation}%6.3f, Min: ${stats.getMin}%6.3f, Max: ${stats.getMax}%6.3f"
  }

  var hqlCount = -1

  def nextHQLFilename(): String = {
    hqlCount += 1
    s"HQLQuery$hqlCount.txt"
  }

  var oqlCount = -1

  def nextOQLFilename(): String = {
    oqlCount += 1
    s"OQLQuery$oqlCount.txt"
  }

  def valuesToString(stats: DescriptiveStatistics): String = {
    stats.getValues.map(d => f"$d%2.3f").mkString(", ")
  }

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Performance test driver")
      val repeats = opt[Int]("repeats", default = Some(5), short = 'r')
      val outputDir = opt[String]("outputDir", default = Some(Paths.get(System.getProperty("java.io.tmpdir"), "raw-perf-test").toString), short = 'o')
      val runQueryType = opt[String]("runTypes", default = Some("all"), short = 't')
      val dataset = opt[String]("dataset", default = Some("publications"), short = 'd', descr = "Possible choices: publications, publicationsLarge")
      val saveResults = opt[Boolean]("save-results", default = Some(false), short = 's', descr = "Save the results of the query")
      val queryFile = trailArg[String](required = true)
    }

    def timeHQL(hql: String): Option[DescriptiveStatistics] = {
      logger.info(s"Testing query:\n$hql")

      val p = Paths.get(Conf.outputDir(), nextHQLFilename())
      val br = new PrintStream(Files.newOutputStream(p))
      br.println(s"$hql\n")

      val execTimes = new DescriptiveStatistics()
      var resDF: DataFrame = null
      var res: Array[Row] = null
      for (i <- 1 to Conf.repeats()) {
        val clock = Stopwatch.createStarted()
        resDF = sqlContext.sql(hql)
        res = resDF.collect()
        execTimes.addValue(clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0)
      }

      Console.withOut(br) {
        resDF.explain()
      }
      br.println()

      br.println(s"Execution : ${statsToString(execTimes)}")
      br.println(s"Samples   : ${valuesToString(execTimes)}\n")

      if (Conf.saveResults()) {
        br.println(s"Query result:\n${res.mkString("\n")}")
      }
      br.close()

      Some(execTimes)
    }

    case class OQLResults(compile: DescriptiveStatistics, exec: DescriptiveStatistics, total: DescriptiveStatistics)

    def timeOQL(oql: String, comp: QueryCompilerClient, accessPaths: List[AccessPath[_]]): Option[OQLResults] = {
      logger.info(s"**************** Query:\n$oql")

      val p = Paths.get(Conf.outputDir(), nextOQLFilename())
      val br = new PrintStream(Files.newOutputStream(p))
      br.println(s"$oql\n")

      val compileTimes = new DescriptiveStatistics()
      val execTimes = new DescriptiveStatistics()
      val totals = new DescriptiveStatistics()

      var res: Any = null

      for (i <- 1 to Conf.repeats()) {
        val clock = Stopwatch.createStarted()
        val result = comp.compileOQL(oql, accessPaths) match {
          case Left(error) => {
            logger.warn(s"Error compiling query: $oql. Error: $error")
            return None
          }
          case Right(query: RawQuery) => {
            val compileTime = clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0
            compileTimes.addValue(compileTime)
            clock.reset().start()
            res = query.computeResult
            val execTime = clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0
            execTimes.addValue(execTime)
            totals.addValue(compileTime + execTime)
          }
        }
      }

      br.println(s"Compile  : ${statsToString(compileTimes)}")
      br.println(s"Samples  : ${valuesToString(execTimes)}\n")

      br.println(s"Execution: ${statsToString(execTimes)}")
      br.println(s"Samples  : ${valuesToString(execTimes)}\n")

      br.println(s"Total    : ${statsToString(totals)}")
      br.println(s"Samples  : ${valuesToString(totals)}\n")

      if (Conf.saveResults()) {
        br.println(s"Query result:\n${convertToJson(res)}")
      }
      br.close()

      Some(OQLResults(compileTimes, execTimes, totals))
    }

    def writeSparkConfiguration(path: Path) = {
      val sparkConfigBW = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
      sparkConfigBW.write(sc.getConf.getAll.sorted.mkString("\n"))
      sparkConfigBW.newLine()
      sparkConfigBW.write(sqlContext
        .getAllConfs
        .map(t => s"${t._1} ->  ${t._2}")
        .mkString("\n"))
      sparkConfigBW.close()
    }


    val runOQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "oql"
    val runHQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "hql"

    writeSparkConfiguration(Paths.get(Conf.outputDir(), "sparkConfig.txt"))

    val queries = Queries.readQueries(Conf.queryFile())
    val resDir = Paths.get(Conf.outputDir())
    if (!Files.isDirectory(resDir)) {
      logger.info(s"Creating results directory: $resDir")
      Files.createDirectory(resDir)
    }

    val hqlResults = new ArrayBuffer[String]()
    if (runHQL) {
      // Load and register as tables the data sources
      readJson("data/publications/authors.json", "authors")
      Conf.dataset() match {
        case "publications" => readJson("data/publications/publications.json", "publications")
        case "publicationsLarge" => readJson("data/publications/publicationsLarge.json", "publications")
        case _ => throw new IllegalArgumentException("Invalid dataset")
      }

      // Execute the tests
      for (q <- queries) {
        if (q.hql != "") {
          timeHQL(q.hql) match {
            case Some(qr) => hqlResults += s"    Total:   ${statsToString(qr)}"
            case None => hqlResults += "Fail"
          }
        }
      }
    }

    val oqlResults = new ArrayBuffer[String]()
    if (runOQL) {
      val ds: List[Dataset[_]] = Conf.dataset() match {
        case "publications" => Publications.loadPublications(sc)
        case "publicationsLarge" => Publications.loadPublicationsLarge(sc)
        case _ => throw new IllegalArgumentException("Invalid dataset")
      }
      val accessPaths = ds.map(ds => ds.accessPath)
      val comp = new QueryCompilerClient(rawClassLoader)
      setDockerHost()
      for (q <- queries) {
        if (q.oql != "") {
          timeOQL(q.oql, comp, accessPaths) match {
            case Some(queryRes) => oqlResults +=
              s"    Compile: ${statsToString(queryRes.compile)}\n" +
                s"    Exec:    ${statsToString(queryRes.exec)}\n" +
                s"    Total:   ${statsToString(queryRes.total)}"

            case None => oqlResults += "Fail"
          }
        }
      }
    }

    val p = Paths.get(Conf.outputDir(), "summary.txt")
    val summaryFileBW = new PrintStream(Files.newOutputStream(p))
    summaryFileBW.println(s"Configuration:\n${Conf.summary}")
    // Write the results to the summary file
    for (i <- 0 until queries.size) {
      summaryFileBW.println(s"Query $i:  ${queries(i).name}")
      if (!oqlResults.isEmpty) {
        summaryFileBW.println(s"  OQL:\n${oqlResults(i)}")
      }
      if (!hqlResults.isEmpty) {
        summaryFileBW.println(s"  HQL:\n${hqlResults(i)}")
      }
      summaryFileBW.println()
    }
    summaryFileBW.close()
  }
}
