package raw.experiments

import java.io.PrintStream
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{RangePartitioner, SparkContext}
import raw.repl.RawSparkContext

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


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

  val sparkContext: SparkContext = new RawSparkContext().sc

  val pubs: RDD[Publication] = Common.newRDDFromJSON[Publication]("data/pubs-authors/publications.json", sparkContext)
  val authors: RDD[Author] = Common.newRDDFromJSON[Author]("data/pubs-authors/authors.json", sparkContext)

  def outAndFile(str: String): Unit = {
    println(str)
    outFile.println(str)
    outFile.flush()
  }

  val repetitions = 10

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
    outAndFile(rdd.take(10).map(_.toString.take(80)).mkString("\n"))
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


  def articlesAllAuthorsSameAgeNoBcast(): RDD[Publication] = {
    val authorsAges: RDD[(String, Int)] = authors.map(a => (a.name, a.year))

    val pubsKeyed: RDD[(String, Publication)] = pubs
      .flatMap(p => p.authors.map(authorName => (authorName, p)))
      .cache()

    val j1: RDD[(String, (Publication, Int))] = pubsKeyed.join(authorsAges)
    val pubsToAuthorAges: RDD[(Publication, Iterable[Int])] = j1.values.groupByKey()

    pubsToAuthorAges
      .filter({ case (p, authorAges) => authorAges.toSet.size == 1 })
      .keys
  }

  case class QueryResult(article: String, phd: String, phdYear: Int, prof: String, profYear: Int)
  implicit object PublicationOrdering extends Ordering[Publication] {
    def compare(a:Publication, b:Publication) = a.title compare b.title
  }

  def extractMatchingAuthorPairs(p: Publication, authors: Map[String, Author]): ArrayBuffer[QueryResult] = {
    // Extract list of PhDs and of professor authors
    val phDsAuthors = new ArrayBuffer[Author]()
    val profAuthors = new ArrayBuffer[Author]()
    p.authors.foreach(name => authors.get(name) match {
      case Some(a@Author(name, "PhD", _)) => phDsAuthors += a
      case Some(a@Author(name, "professor", _)) => profAuthors += a
      case _ =>
    })

    for {
      phd <- phDsAuthors
      prof <- profAuthors
      if (prof.year > phd.year)
    } yield new QueryResult(p.title, phd.name, phd.year, prof.name, prof.year)
  }

  def oneProfOneStudentProfYoungerThanStudentWithBcastVar(): RDD[QueryResult] = {
    // First stage: obtain a map from author name to author for all authors who are either professor or PhDs 
    val authorsMap: Map[String, Author] = authors
      .filter(a => a.title == "PhD" || a.title == "professor")
      .map(a => a.name -> a)
      .collectAsMap()
    logger.info("AuthorsMap: {}", authorsMap.mkString("\n"))
    // Broadcast to all nodes
    val bcasted = sparkContext.broadcast(authorsMap)
    // for each publication, generate zero or more entries with the list of matching pairs of phd-professor
    // authors where the professor is younger than the phd.
    pubs.mapPartitions(iter => {
      iter.flatMap(p => extractMatchingAuthorPairs(p, bcasted.value))
    }
    ).sortBy(_.article)
  }

  def oneProfOneStudentProfYoungerThanStudentFullDistributed(): RDD[QueryResult] = {
    val phdAuthors: RDD[(String, Author)] = authors
      .filter(a => a.title == "PhD")
      .map(a => a.name -> a)

    val profAuthors: RDD[(String, Author)] = authors
      .filter(a => a.title == "professor")
      .map(a => a.name -> a)

    val pubsKeyed: RDD[(String, Publication)] = pubs
      .flatMap(p => p.authors.map(authorName => (authorName, p)))
      .cache()

    val j1: RDD[(String, (Publication, Author))] = pubsKeyed.join(phdAuthors)
    val pubsToPhD: RDD[(Publication, Iterable[Author])] = j1.values.groupByKey()

    val j2: RDD[(String, (Publication, Author))] = pubsKeyed.join(profAuthors)
    val pubsToProf: RDD[(Publication, Iterable[Author])] = j2.values.groupByKey()

    val j: RDD[(Publication, (Iterable[Author], Iterable[Author]))] = pubsToPhD.join(pubsToProf)
    j.flatMap({ case (pub, (phdsAuthors, profAuthors)) => {
      val l: Iterable[QueryResult] = for {
        phd <- phdsAuthors
        prof <- profAuthors
        if (prof.year > phd.year)
      } yield new QueryResult(pub.title, phd.name, phd.year, prof.name, prof.year)
      l.toSeq
    }
    }).sortBy(_.article)
  }

  def oneProfOneStudentProfYoungerThanStudentFullDistributed2(): RDD[QueryResult] = {
    val authorsKeyed: RDD[(String, Author)] = authors
      .filter(a => a.title == "PhD" || a.title == "professor")
      .map(a => a.name -> a)

    val pubsKeyed: RDD[(String, Publication)] = pubs
      .flatMap(p => p.authors.map(authorName => (authorName, p)))

    val authorNameToPubAuthorTuple: RDD[(String, (Publication, Author))] = pubsKeyed.join(authorsKeyed)

    //    val c1 =
    //    createCombiner: V => C,
    //    mergeValue: (C, V) => C,
    //    mergeCombiners: (C, C) => C,
    val pubsToAuthors: RDD[(Publication, Iterable[Author])] = authorNameToPubAuthorTuple.values
      .groupByKey()

    pubsToAuthors.flatMap({ case (pub, authors) => {
      val l: Iterable[QueryResult] = for {
        phd <- authors.filter(_.title == "PhD")
        prof <- authors.filter(_.title == "professor")
        if (prof.year > phd.year)
      } yield new QueryResult(pub.title, phd.name, phd.year, prof.name, prof.year)
      l.toSeq
    }
    }).sortBy(_.article)
  }

  // Reimplement queries above without using a broadcast variable, just joins. Compare time.

  def main(args: Array[String]) {
    // val res = queryArticlesWithMoreThanOnePhDStudent()
    //     val res = articlesAllAuthorsSameAgeNoBcast()
    //    val res = oneProfOneStudentProfYoungerThanStudent()

    //    doTest(oneProfOneStudentProfYoungerThanStudentFullDistributed())
    //    doTest(oneProfOneStudentProfYoungerThanStudentFullDistributed2())
    //    doTest(oneProfOneStudentProfYoungerThanStudentWithBcastVar())


    val res = oneProfOneStudentProfYoungerThanStudentFullDistributed2()
    Common.outAndFile(res.toDebugString)
    Common.outAndFile("Result size: " + res.count())
    val localResults = res.collect()
    val uniqueNames = localResults.map(q => q.article).distinct
    outFile.append(s"Articles: ${uniqueNames.length}\n" + uniqueNames.mkString("\n"))
    //    Common.outAndFile("result: " +
    //      localResults
    //        .map(q => f"${q.article}%20s ${q.phd}%10s ${q.phdYear}%5d ${q.prof}%10s ${q.profYear}%5d})")
    //        .mkString("\n"))
    //  }val localResults: Array[QueryResult] = res.collect()
    //    val uniqueNames = localResults.map(q => q.article).distinct
    //    Common.outAndFile(s"Articles: ${uniqueNames.length}\n" + uniqueNames.mkString("\n"))
    //    Common.outAndFile("result: " +
    //      localResults
    //        .map(q => f"${q.article}%20s ${q.phd}%10s ${q.phdYear}%5d ${q.prof}%10s ${q.profYear}%5d})")
    //        .mkString("\n"))
    outFile.close()

//    Thread.sleep(10000000)
  }
}