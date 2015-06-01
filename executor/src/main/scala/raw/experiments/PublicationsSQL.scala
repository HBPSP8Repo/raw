package raw.experiments

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import raw.repl.RawSparkContext

import scala.collection.mutable.ArrayBuffer

object PublicationsSQL extends StrictLogging {

  def authors(rdd: DataFrame) = {
    val v: DataFrame = rdd.select("authors").limit(10)
    val rows: Array[Row] = v.collect()
    val s: Array[ArrayBuffer[String]] = rows.map(row => row.getAs[ArrayBuffer[String]](0))
    s.foreach(ab => println("authors: " + ab.mkString("; ")))
  }

  // select doc from doc in Documents where "particle detectors" in controlledterms
  def query1(hiveContext: HiveContext, pubs: DataFrame): Unit = {
    val res = hiveContext.sql( """ SELECT title FROM pubs WHERE array_contains(controlledterms, "particle detectors") """)
    Common.printRDD(res)
  }

  val hiveContext: HiveContext = {
    val rawSparkContext = new RawSparkContext
    new HiveContext(rawSparkContext.sc)
  }

  val pubs: DataFrame = Common.newDataframeFromJSON("data/publications.json", hiveContext)

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
  }
}
