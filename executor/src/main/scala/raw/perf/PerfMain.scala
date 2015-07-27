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
import raw.QueryLogger
import raw.datasets.patients.Patients
import raw.datasets.publications.Publications
import raw.datasets.{AccessPath, Dataset}
import raw.executionserver.{DefaultSparkConfiguration, RawMutableURLClassLoader, ResultConverter}
import raw.perf.Queries.{PatientsDS, PublicationsDS, PublicationsLargeDS}
import raw.utils.RawUtils

import scala.collection.mutable.ArrayBuffer

object Queries {

  abstract sealed class DatasetType

  case class PublicationsDS() extends DatasetType

  case class PublicationsLargeDS() extends DatasetType

  case class PatientsDS() extends DatasetType

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

  def valuesToString(stats: DescriptiveStatistics): String = {
    stats.getValues.map(d => f"$d%2.3f").mkString(", ")
  }

  def toString(stats: DescriptiveStatistics) = {
    s"Compile  : ${statsToString(stats)}\n" +
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

      br.println(toString(execTimes))

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

      br.println(toString(compileTimes))
      br.println(toString(execTimes))
      br.println(toString(totals))

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

    val dataset = Conf.dataset() match {
      case "publications" => PublicationsDS
      case "publicationsLarge" => PublicationsLargeDS
      case "patients" => PatientsDS
      case _ => throw new IllegalArgumentException("Invalid dataset")
    }

    val queries = Queries.readQueries(Conf.queryFile())

    val outputDir = Paths.get(Conf.outputDir())
    RawUtils.cleanOrCreateDirectory(outputDir)

    writeSparkConfiguration(outputDir.resolve("sparkConfig.txt"))

    val runOQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "oql"
    val runHQL = Conf.runQueryType() == "all" || Conf.runQueryType() == "hql"

    val hqlResults = new ArrayBuffer[String]()
    if (runHQL) {
      // Load and register as tables the data sources
      dataset match {
        case PublicationsDS =>
          readJson("data/publications/publications.json", "publications")
          readJson("data/publications/authors.json", "authors")
        case PublicationsLargeDS =>
          readJson("data/publications/publicationsLarge.json", "publications")
          readJson("data/publications/authors.json", "authors")
        case PatientsDS =>
          readJson("data/patients/patients.json", "patients")
      }

      // Execute the tests
      for (q <- queries) {
        val res: String = q.hql match {
          case s: String if s == null || s == "" => "Not run"
          case s: String =>
            timeHQL(s) match {
              case Some(qr) => s"    Total:   ${statsToString(qr)}"
              case None => "Fail"
            }
        }
        hqlResults += res
      }
    }

    val oqlResults = new ArrayBuffer[String]()
    if (runOQL) {
      val ds: List[Dataset[_]] = dataset match {
        case PublicationsDS => Publications.loadPublications(sc)
        case PublicationsLargeDS => Publications.loadPublicationsLarge(sc)
        case PatientsDS => Patients.loadPatients(sc)
      }
      val accessPaths = ds.map(ds => ds.accessPath)
      val comp = new QueryCompilerClient(rawClassLoader, outputDir)
      setDockerHost()
      for (q <- queries) {
        val res = q.oql match {
          case s: String if s == null || s == "" => "Not run"
          case s: String =>
            timeOQL(s, comp, accessPaths) match {
              case Some(queryRes) =>
                s"    Compile: ${statsToString(queryRes.compile)}\n" +
                  s"    Exec:    ${statsToString(queryRes.exec)}\n" +
                  s"    Total:   ${statsToString(queryRes.total)}"
              case None => "Fail"
            }
        }
        oqlResults += res
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
