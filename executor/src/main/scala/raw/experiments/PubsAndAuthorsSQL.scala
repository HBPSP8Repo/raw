package raw.experiments

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import raw.repl.RawSparkContext

object PubsAndAuthorsSQL extends StrictLogging {

  val hiveContext: HiveContext = {
    val rawSparkContext = new RawSparkContext
    new HiveContext(rawSparkContext.sc)
  }

  val pubs: DataFrame = Common.newDataframeFromJSON(hiveContext, "data/pubs-authors/publicationsSmall.json", "publications")
  val authors: DataFrame = Common.newDataframeFromJSON(hiveContext, "data/pubs-authors/authors.json", "authors")

  def main(args: Array[String]) {
    logger.info("Create rdd: " + pubs)
    logger.info("Create rdd: " + authors)

    // Articles which have more than one PhD student as an author.
    //    Common.runQuery( """
    //          SELECT * FROM (
    //              SELECT publications.title, COUNT(*) as phdAuthors
    //              FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
    //              JOIN authors ON (authors.name = authorsExp AND authors.title = "PhD")
    //              GROUP BY publications.title
    //              ) AS t1
    //          WHERE phdAuthors > 1
    //                     	    """, repetitions = 0)

    //    Give me the articles whose authors are all the same age :) ?
    //    Common.runQuery( """
    //            SELECT * FROM (
    //              SELECT publications.title, COUNT(DISTINCT authors.year) distinctYears
    //              FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
    //              JOIN authors ON (authors.name = authorsExp)
    //              GROUP BY publications.title) as t1
    //            WHERE distinctYears = 1
    //                     """, repetitions = 0)

    // Articles where one is a prof, one is a PhD student, but the prof is younger than the PhD student.
    Common.runQuery(
      """
SELECT phds.title, phds.name as phdName, phds.year as phdYear, profs.name as profName, profs.year as profYear
   FROM
      (SELECT publications.title, authors.name, authors.year
        FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
        JOIN authors ON (authors.name = authorsExp and (authors.title = "PhD"))) phds
   JOIN
      (SELECT publications.title, authors.name, authors.year
        FROM publications LATERAL VIEW explode(publications.authors) newTable AS authorsExp
        JOIN authors ON (authors.name = authorsExp and (authors.title = "professor"))) profs
   ON (phds.title = profs.title)
   WHERE (phds.year < profs.year)
   ORDER BY phds.title
      """, repetitions = 0)
  }

//  Thread.sleep(1000000)

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
  //    	    (SELECT title, size(authors) AS authCount FROM publications) t2
}
