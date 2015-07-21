package raw.perf

import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.rogach.scallop.{ScallopConf, ScallopOption}
import raw.RawQuery
import raw.datasets.Dataset
import raw.datasets.publications.Publications
import raw.executionserver.{ResultConverter, DefaultSparkConfiguration, RawMutableURLClassLoader}


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

  lazy val rawClassLoader = {
    val tmp = new RawMutableURLClassLoader(new Array[URL](0), PerfMain.getClass.getClassLoader)
    Thread.currentThread().setContextClassLoader(tmp)
    tmp
  }

  lazy val sc: SparkContext = {
    logger.info("Starting SparkContext with configuration:\n{}", DefaultSparkConfiguration.conf.toDebugString)
    new SparkContext("local[4]", "test", DefaultSparkConfiguration.conf)
  }

  lazy val ds: List[Dataset[_]] = Publications.loadPublications(sc)
  lazy val accessPaths = ds.map(ds => ds.accessPath)
  lazy val comp = new QueryCompilerClient(rawClassLoader)

  def statsToString(stats: DescriptiveStatistics): String = {
    f"${stats.getMean}%2.3f, StdDev: ${stats.getStandardDeviation}%2.3f, Min: ${stats.getMin}%2.3f, Max: ${stats.getMax}%2.3f"
  }

  var queryCount = 0

  def nextFileName(): String = {
    queryCount += 1
    s"Query$queryCount.txt"
  }

  def valuesToString(stats: DescriptiveStatistics): String = {
    stats.getValues.map(d => f"$d%2.3f").mkString(", ")
  }

  def timeQuery(oql: String, repeats: Int, outDir: String): Unit = {
    logger.info(s"**************** Query:\n$oql")

    val p = Paths.get(outDir, nextFileName())
    val br = Files.newBufferedWriter(p, StandardCharsets.UTF_8)

    br.write(s"$oql\n\n")

    val compileTimes = new DescriptiveStatistics()
    val execTimes = new DescriptiveStatistics()

    var res: Any = null

    for (i <- 1 to repeats) {
      val clock = Stopwatch.createStarted()
      val result = comp.compileOQL(oql, accessPaths) match {
        case Left(error) => {
          logger.warn(s"Error compiling query: $oql. Error: $error")
          return
        }
        case Right(query: RawQuery) => {
          compileTimes.addValue(clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0)
          clock.reset().start()
          res = query.computeResult
          execTimes.addValue(clock.elapsed(TimeUnit.MILLISECONDS) / 1000.0)
        }
      }
    }

    logger.info(s"Compile: ${valuesToString(compileTimes)}")
    br.write(s"Compile: ${valuesToString(compileTimes)}\n")

    logger.info(s"Execution: ${valuesToString(execTimes)}")
    br.write(s"Execution: ${valuesToString(execTimes)}\n\n")

    logger.info(s"Compile: ${statsToString(compileTimes)}")
    br.write(s"Compile: ${statsToString(compileTimes)}\n")

    logger.info(s"Execution: ${statsToString(execTimes)}")
    br.write(s"Execution: ${statsToString(execTimes)}\n\n")

    br.write(s"Query result:\n${convertToJson(res)}\n")
    br.close()
  }

  def loadQueries(filename: String): Array[String] = {
    val path = Paths.get(filename)
    logger.info(s"Opening query file: $path")
    val lines = scala.io.Source.fromFile(path.toFile).mkString
    lines.split("\n\n").map(_.trim)
  }

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Performance test driver")
      val repeats: ScallopOption[Int] = opt[Int]("repeats", default = Some(5), short = 'r')
      val resultDir: ScallopOption[String] = opt[String]("resultsDir", default = Some("/tmp"), short = 'd')
      val queryFile: ScallopOption[String] = trailArg[String](required = true)
    }

    val queries = loadQueries(Conf.queryFile())

    setDockerHost()
    for (q <- queries) {
      timeQuery(q, Conf.repeats(), Conf.resultDir())
    }
  }
}
