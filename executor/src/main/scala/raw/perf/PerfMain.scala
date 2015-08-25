package raw.perf

import java.io.PrintStream
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.rogach.scallop.ScallopConf
import raw.datasets.AccessPath
import raw.executionserver.{DefaultSparkConfiguration, RawMutableURLClassLoader, ResultConverter}
import raw.utils.{DockerUtils, RawUtils}

import scala.collection.TraversableLike
import scala.collection.mutable.ArrayBuffer

object PerfMainUtils {

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

  def statsToString(stats: DescriptiveStatistics): String = {
    f"${stats.getMean}%6.3f, StdDev: ${stats.getStandardDeviation}%6.3f, Min: ${stats.getMin}%6.3f, Max: ${stats.getMax}%6.3f"
  }

  def valuesToString(stats: DescriptiveStatistics): String = {
    stats.getValues.map(d => f"$d%2.3f").mkString(", ")
  }

  def resultsToString(stats: DescriptiveStatistics) = {
    s"${statsToString(stats)}\n" +
      s"Samples  : ${valuesToString(stats)}\n"
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
}

/* From SBT, project executor:
 * run -r 1 -t oql -d publications  src\main\resources\perf\trial.xml
 */
object PerfMain extends StrictLogging with ResultConverter {

  import PerfMainUtils._

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

  //  def readJson(resource: String, tableName: String): DataFrame = {
  //    val url = Resources.getResource(resource)
  //    val fullpath = Paths.get(url.toURI).toString
  //    val df = sqlContext.read.json(fullpath)
  //    df.registerTempTable(tableName)
  //    logger.info(s"Loaded ${df.count()} rows from $resource. Registered as $tableName")
  //    df
  //  }

  def registerTable[T <: scala.Product](ap: AccessPath[T]): DataFrame = {
    val df: DataFrame = sqlContext.implicits.rddToDataFrameHolder(ap.path.right.get)(ap.tag).toDF()
    df.registerTempTable(ap.name)
    logger.info(s"Loaded ${df.count()} rows. Registered as ${ap.name}")
    df
  }

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Performance test driver")
      val repeats = opt[Int]("repeats", default = Some(5), short = 'r')
      val outputDir = opt[String]("outputDir", default = Some(Paths.get(System.getProperty("java.io.tmpdir"), "raw-perf-test").toString), short = 'o')
      val runQueryType = opt[String]("runTypes", default = Some("all"), short = 't')
      val dataset = opt[String]("dataset", default = Some("publications"), short = 'd', descr = "Possible choices: publications, publicationsLarge, patients")
      val saveResults = opt[Boolean]("save-results", default = Some(false), short = 's', descr = "Save the results of the query")
      val queryFile = trailArg[String](required = true)
    }

    case class HQLResults(exec: DescriptiveStatistics, numberResults: Int)

    def timeHQL(hql: String): Option[HQLResults] = {
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

      br.println("Execution: " + resultsToString(execTimes))

      if (Conf.saveResults()) {
        br.println(s"Query result:\n${res.mkString("\n")}")
      }
      br.close()

      Some(HQLResults(execTimes, res.length))
    }

    case class OQLResults(compile: DescriptiveStatistics, exec: DescriptiveStatistics, total: DescriptiveStatistics, numberResults: Int)

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
        try {
          // Compile
          val clock = Stopwatch.createStarted()
          val query = comp.compileOQL(oql, accessPaths)
          val compileTime = clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0
          compileTimes.addValue(compileTime)

          // Execute
          clock.reset().start()
          res = query.computeResult
          val execTime = clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0
          execTimes.addValue(execTime)
          totals.addValue(compileTime + execTime)
        } catch {
          case ex: RuntimeException =>
            logger.warn(s"Error compiling query: $oql. Exception: $ex")
            return None
        }
      }

      br.println("Compile  : " + resultsToString(compileTimes))
      br.println("Execution: " + resultsToString(execTimes))
      br.println("Total    : " + resultsToString(totals))

      val size = if (res.isInstanceOf[Array[_]]) {
        res.asInstanceOf[Array[_]].length
      } else if (res.isInstanceOf[TraversableLike[_, _]]) {
        res.asInstanceOf[TraversableLike[_, _]].size
      } else {
        logger.warn(s"Result is not a sequence or an array: ${res.getClass}.")
        1
      }

      br.println(s"Number of results: $size")

      if (Conf.saveResults()) {
        br.println(s"Query result:\n${convertToJson(res)}")
      } else {
        val sampleSize = 2
        val resHead = if (res.isInstanceOf[Array[_]]) {
          res.asInstanceOf[Array[_]].take(sampleSize)
        } else if (res.isInstanceOf[TraversableLike[_, _]]) {
          res.asInstanceOf[TraversableLike[_, _]].take(sampleSize)
        } else {
          res
        }
        br.println(s"Query result:\n${convertToJson(resHead)}")
      }
      br.close()

      Some(OQLResults(compileTimes, execTimes, totals, size))
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


    val queries = readQueries(Conf.queryFile())

    val outputDir = Paths.get(Conf.outputDir())
    RawUtils.cleanOrCreateDirectory(outputDir)

    writeSparkConfiguration(outputDir.resolve("sparkConfig.txt"))

    val runOQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "oql"
    val runHQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "hql"


    val hqlResults = new ArrayBuffer[String]()
    if (runHQL) {
      val datasets = AccessPath.loadSparkDataset(Conf.dataset(), sc)
      datasets.foreach(ds => registerTable(ds))

      // Execute the tests
      for (q <- queries) {
        val res: String = q.hql match {
          case s: String if s == null || s == "" => "Not run"
          case s: String =>
            timeHQL(s) match {
              case Some(qr) =>
                s"    Total:   ${statsToString(qr.exec)}\n" +
                  s"    #results: ${qr.numberResults}"
              case None => "Fail"
            }
        }
        hqlResults += res
      }
    }

    val oqlResults = new ArrayBuffer[String]()
    if (runOQL) {
      val aps = AccessPath.loadSparkDataset(Conf.dataset(), sc)
      val comp = new QueryCompilerClient(rawClassLoader, outputDir)
      DockerUtils.setEnvironment()
      DockerUtils.startDocker()
      try {
        for (q <- queries) {
          val res = q.oql match {
            case s: String if s == null || s == "" => "Not run"
            case s: String =>
              timeOQL(s, comp, aps) match {
                case Some(queryRes) =>
                  s"    Compile: ${statsToString(queryRes.compile)}\n" +
                    s"    Exec:    ${statsToString(queryRes.exec)}\n" +
                    s"    Total:   ${statsToString(queryRes.total)}\n" +
                    s"    #results: ${queryRes.numberResults}"
                case None => "Fail"
              }
          }
          oqlResults += res
        }
      } finally {
        DockerUtils.stopDocker()
      }
    }

    val p = outputDir.resolve("summary.txt")

    // Save the file with the queries.
    val qFile = Paths.get(Conf.queryFile())
    Files.copy(qFile, outputDir.resolve(qFile.getFileName()))

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
